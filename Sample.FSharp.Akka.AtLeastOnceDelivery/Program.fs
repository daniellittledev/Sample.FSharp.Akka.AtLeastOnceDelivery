// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

open System
open Akka.Actor
open Akka.Persistence
open Akkling
open Akkling.Persistence
open Serilog
open Akka.Event
open System.Threading.Tasks

// Wish List
(*
 - Auto ack after receiving message with a DeliveryId
  - Normal actors should ack straight away
  - Persisting actors should ack after a persist
 - 
*)
// End

let timeout = Some <| TimeSpan.FromSeconds(1.0)

// module Envolopes

type Metadata =
    {
         MessageId: Guid;
         ConversationId: Guid;
         DeliveryId: int64 option;
         Timestamp: DateTimeOffset
    }

type IEnvolope =
   abstract member Metadata: Metadata with get
   abstract member Payload: obj with get

(*
type IEnvolope<in 't> =
   abstract member Metadata: Metadata with get
   abstract member Payload: 't with get
*)

type Envolope<'t> = 
    {
        Metadata: Metadata;
        Payload: 't
    }
    interface IEnvolope with
        member this.Metadata with get() = this.Metadata
        member this.Payload with get() = this.Payload :> obj
    
let metadata () =
    {
        MessageId = Guid.NewGuid();
        ConversationId = Guid.NewGuid();
        DeliveryId = None;
        Timestamp = DateTimeOffset.Now;
    }

let nextMetadata prevMetadata deliveryId =
    {
        MessageId = Guid.NewGuid();
        ConversationId = prevMetadata.ConversationId;
        DeliveryId = deliveryId;
        Timestamp = DateTimeOffset.Now;
    } 

let envolopeDynamic (message:obj) nextMetadata : IEnvolope =
    match message with
    | :? IEnvolope as envolope -> envolope
    | message ->
        let envolopeTypeConstructor = typedefof<Envolope<_>>
        let payloadType = message.GetType()
        let recordType = envolopeTypeConstructor.MakeGenericType(payloadType);
        let envolope = FSharp.Reflection.FSharpValue.MakeRecord(recordType, [| nextMetadata; message |])
        envolope :?> IEnvolope

let envolopeStrong message nextMetadata = 
    {
        Metadata = nextMetadata;
        Payload = message;
    }

let deliver (deliverer:AtLeastOnceDeliverySemantic) isRecovering (actorRef:Akka.Actor.IActorRef) prevMetadata messageToSend = 
    let addDeliveryId deliveryId = 
        envolopeDynamic messageToSend <| nextMetadata prevMetadata (Some(deliveryId))
    deliverer.Deliver(actorRef.Path, addDeliveryId, isRecovering)

let deliverDynamic = 
    ()

let (|Envolope|_|) (message: obj) =
    if message :? IEnvolope
        then Some (message :?> IEnvolope)
    else None

// Random stuff

let crashUntil (name:string) limit = 
    let mutable count = 0
    let crashUntilInner (log:ILogger) =
        count <- count + 1
        if count < limit then
            log.Information("Crashing! {Name}", name)
            failwith "Eeep"
        else ()
    crashUntilInner

type Acknowledgement = | Ack

let createDeliver mailbox = 
    let deliverer = AtLeastOnceDelivery.createDefault mailbox
    deliverer.Receive (upcast mailbox) (upcast ReplaySucceed) |> ignore
    deliverer

let ask (metadata:Metadata) message (actorRef:IActorRef<_>) = //:Akka.Actor.IActorRef)
    let message = (envolopeStrong message <| nextMetadata metadata None)
    actorRef.Ask(message, timeout)

    //actorRef.Ask<'T>(message, match timeout with | Some(value) -> Nullable(value) | _ -> Nullable() ) |> Async.AwaitTask

let tell (metadata:Metadata) message actorRef = 
    actorRef <! (envolopeStrong message <| nextMetadata metadata None)

let ackMessage (mailbox:Actor<_>) (metadata:Metadata) =
    match metadata.DeliveryId with
    | Some(deliveryId) -> 
        let message = envolopeStrong Ack <| nextMetadata metadata (Some(deliveryId))
        mailbox.Sender() <! message
        Unhandled :> Effect<obj>
    | _ -> Unhandled :> Effect<obj>

type RecievedMessage = 
    | Recieved of IEnvolope

let delivererReceive (log:ILogger) (deliverer:AtLeastOnceDeliverySemantic) (mailbox:Eventsourced<_>) (message:obj) name =
    let effect = 
        match message with
        // Hacking this to trigger it all the time, so suppressing it here
        | :? PersistentLifecycleEvent -> Unhandled :> Effect<_>
        // Ack a RecievedMessage
        | :? RecievedMessage as recieved ->
            let (Recieved envolope) = recieved
            match envolope with 
            // If the item is already confirmed Confirm returns false which is fine
            | :? Envolope<Acknowledgement> as ack -> 
                log.Information("Delivery confirmed for id {deliveryId}, actor {Actor}", ack.Metadata.DeliveryId.Value, name)
                deliverer.Confirm (ack.Metadata.DeliveryId.Value) |> ignored
            | _ ->
                if not <| mailbox.IsRecovering() then
                    match envolope.Metadata.DeliveryId with
                    | Some(deliveryId) ->
                        log.Information("Acknowledging delivery {deliveryId}, actor {Actor}", deliveryId, name)
                        ackMessage mailbox envolope.Metadata
                    | _ -> Unhandled :> Effect<_>
                else 
                    Unhandled :> Effect<_>
        // Returns an effect detailing if the message has already been handled
        | _ -> deliverer.Receive (upcast mailbox) message
    effect


// module Actors

type TestMessages = 
    | Command
    | Event

let anyio (a:IO<obj>) = a

// Persisted Reliable Actor...


let reliableActor (log:ILogger) (nextActor:Akka.Actor.IActorRef) = (propsPersist(fun mailbox ->
    let deliverer = createDeliver mailbox

    let rec loop state = 
        actor {
            let! anyMessage = mailbox.Receive()

            
            let! something = actor {
                let! anyMessage = mailbox.Receive()

                return Become(fun message ->
                    // do some work
                    Ignore :> Effect<_>
                )
            }

            // Let the AtLeastOnceDeliverySemantic have a go at the message
            let effect = delivererReceive log deliverer mailbox anyMessage "Reliable"

            if effect.WasHandled()
            then return effect
            else
                log.Information("Reliable actor handling {Message}", anyMessage)
                match anyMessage with
                // This is currently always going to be null as we're not creating any snapshots
                | SnapshotOffer snap ->
                    deliverer.DeliverySnapshot <- snap

                | :? IEnvolope as message ->
                    return Persist(upcast Recieved(message))

                | :? RecievedMessage as event ->
                    let (Recieved envolope) = event
                    let messageToSend = envolope.Payload
                    deliver deliverer (mailbox.IsRecovering()) nextActor (envolope.Metadata) messageToSend |> ignore

                | _ -> return Unhandled
        }
    loop () ))

//


type Payload = obj

type DeliveryId = int64

type EventType = EventType of string

type IActorRef = Akka.Actor.IActorRef

type EventBusCommand =
    | Publish of Payload
    | Subscribe of EventType * IActorRef
    | Unsubscribe of EventType * IActorRef

    (*
type EventBusEvent =
    | Published of Payload
    | Confirmed of DeliveryId
    | Subscribed of EventType * IActorRef
    | Unsubscribed of EventType * IActorRef
    *)

type Delivery = { Payload: Payload; DeliveryId: DeliveryId }

type Subscribers = 
    {
        EventHandlers: Map<EventType, IActorRef list>;
    }

type Done = Done

let addItemToMapList key item map =
    let list = 
        if map |> Map.containsKey key
        then item :: (map |> Map.find key)
        else [item]
    map |> Map.add key list

let removeItemFromMapList key item map =
    if map |> Map.containsKey key
    then
        let list = map |> Map.find key
        let shortList = list |> List.except [item]
        map |> Map.add key shortList
    else
        map

let eventBusActor (log:ILogger) = (propsPersist(fun mailbox ->
    let deliverer = createDeliver mailbox

    let rec loop state = 
        actor {
            let! anyMessage = mailbox.Receive()

            // Let the AtLeastOnceDeliverySemantic have a go at the message
            let effect = delivererReceive log deliverer mailbox anyMessage "eventBus"

            // The AtLeastOnceDelivery Receive may have already handled the message, such as in the case of recovery
            if effect.WasHandled() then return effect
            else
                log.Information("EventBus actor handling {Message}", anyMessage)
                match anyMessage with

                // This is currently always going to be null as we're not creating any snapshots
                | SnapshotOffer snap ->
                    deliverer.DeliverySnapshot <- snap
                
                | :? IEnvolope as envolope -> 
                    return Persist(upcast Recieved(envolope))

                | :? RecievedMessage as event ->
                    let (Recieved envolope) = event
                    match envolope.Payload with
                    | :? EventBusCommand as command ->
                        match command with
                        | Publish(payload) ->
                            let map d = { Payload = payload; DeliveryId = d }

                            let typeName = EventType(payload.GetType().FullName)
                            let delivererFailed = 
                                match state.EventHandlers |> Map.tryFind typeName with
                                | Some actors -> 
                                    actors |> Seq.exists (fun actor -> not <| (deliver deliverer (mailbox.IsRecovering()) actor (envolope.Metadata) payload) )
                                | _ -> false
                        
                            if (delivererFailed) then
                                raise <| MaxUnconfirmedMessagesExceededException("The deliverer failed to send a message")
                        
                            return! loop state
                    
                        | Subscribe(eventType, actor) ->
                            if not <| mailbox.IsRecovering () then mailbox.Sender() <! Done
                            return! loop { EventHandlers = state.EventHandlers |> addItemToMapList eventType actor }

                        | Unsubscribe(eventType, actor) ->
                            if not <| mailbox.IsRecovering () then mailbox.Sender() <! Done
                            return! loop { EventHandlers = state.EventHandlers |> removeItemFromMapList eventType actor }

                    | _ -> return Unhandled

                | _ -> return Unhandled
        }
    loop { EventHandlers = Map.empty }))


let someEventListener (log:ILogger) name crasher = props(fun (mailbox:Actor<_>) ->
    let rec loop () = actor {
        let! (message:obj) = mailbox.Receive ()

        //let envolope = envolope message
        match message with
        | :? IEnvolope as envolope ->

            log.Information("Actor {ActorName} got the message {@Message}", name, envolope)
            ackMessage mailbox (envolope.Metadata) |> ignore
            crasher log
        | _ -> ()

        return! loop ()
    }
    loop ())

let deadLetterAwareActor (log:ILogger) = props(fun (mailbox:Actor<_>) ->
    mailbox.System.EventStream.Subscribe(mailbox.Self, typeof<DeadLetter>) |> ignore
    let rec loop state = 
        actor { 
            let! (msg: DeadLetter) = mailbox.Receive ()
            log.Debug("Message of type {MessageType} with value {@MessageValue} from {Sender} to {Recipient} was not delivered", msg.Message.GetType().FullName, msg.Message, msg.Sender, msg.Recipient)
            return! loop ()
        }
    loop ())

[<EntryPoint>]
let main argv = 
    let log = 
        LoggerConfiguration()
            .WriteTo.ColoredConsole()
            .WriteTo.Seq("http://localhost:5341")
            .MinimumLevel.Verbose()
            .Enrich.WithProperty("Application", "Service")
            .CreateLogger()
    Serilog.Log.Logger <- log

    let configuration = Configuration.defaultConfig ()
    let system = System.create "system" configuration

    spawnAnonymous system <| deadLetterAwareActor log |> ignore

    let crasherA = crashUntil "A" 2
    let crasherB = crashUntil "B" 2

    let someEventListenerRef1 = spawnAnonymous system <| someEventListener log "A" crasherA
    let someEventListenerRef2 = spawnAnonymous system <| someEventListener log "B" crasherB

    let eventBusRef = spawnAnonymous system <| eventBusActor log
    let eventBusRef : IActorRef<Envolope<EventBusCommand>> = eventBusRef |> retype

    let reliableActorRef = spawnAnonymous system <| reliableActor log eventBusRef |> retype


    let mainAsync = async {

        let metadata = (metadata ())

        do! eventBusRef |> ask metadata (Subscribe(EventType(typeof<int32>.FullName), someEventListenerRef1)) |> Async.Ignore
        do! eventBusRef |> ask metadata (Subscribe(EventType(typeof<int32>.FullName), someEventListenerRef2)) |> Async.Ignore

        // Do i need a message Id?

        let message = box 117 
    
        reliableActorRef |> tell metadata (Publish(message)) 

    }

    mainAsync |> Async.RunSynchronously

    Console.Read()
    0 // return an integer exit code
