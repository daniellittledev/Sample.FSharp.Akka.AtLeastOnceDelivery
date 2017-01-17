// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

open System
open Akka.Actor
open Akka.Persistence
open Akkling
open Akkling.Persistence
open Serilog.Core
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

type Envolope<'t> = 
    {
        Metadata: Metadata;
        Payload: 't
    }
    override this.ToString() =
        "Envolope: " + this.Payload.ToString()
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

let inline isNull x = x = Unchecked.defaultof<_>

let normalisedUnionType (anyType:Type) =
    //FSharp.Reflection.FSharpType.IsUnion
    if isNull <| anyType.GetProperty("Tag") then
        anyType
    else
        if isNull <| anyType.BaseType.GetProperty("Tag") then
            anyType
        else
            anyType.BaseType

let envolopeDynamic (message:obj) nextMetadata : IEnvolope =
    match message with
    | :? IEnvolope as envolope -> envolope
    | message ->
        let envolopeTypeConstructor = typedefof<Envolope<_>>
        let payloadType = message.GetType()
        let payloadType = normalisedUnionType payloadType
        let recordType = envolopeTypeConstructor.MakeGenericType(payloadType);
        let envolope = FSharp.Reflection.FSharpValue.MakeRecord(recordType, [| nextMetadata; message |])
        //let ctor = recordType.GetConstructors().[0];
        //let envolope = ctor.Invoke([| nextMetadata; message |])
        envolope :?> IEnvolope

let envolopeStrong message nextMetadata = 
    {
        Metadata = nextMetadata;
        Payload = message;
    }

let deliver (deliverer:AtLeastOnceDeliverySemantic) isRecovering (actorRef:Akka.Actor.IActorRef) metadata message = 
    let addDeliveryId deliveryId = 
        envolopeDynamic message <| nextMetadata metadata (Some(deliveryId))
    deliverer.Deliver(actorRef.Path, addDeliveryId, isRecovering)

let deliverDynamic = 
    ()

let (|Envolope|_|) (message: obj) =
    if message :? IEnvolope
        then Some (message :?> IEnvolope)
    else None

// Random stuff

type Acknowledgement = | Ack

let createDeliver mailbox = 
    let deliverer = AtLeastOnceDelivery.createDefault mailbox
    deliverer.Receive (upcast mailbox) (upcast ReplaySucceed) |> ignore
    deliverer

let ask (metadata:Metadata) message (actorRef:IActorRef<_>) = //:Akka.Actor.IActorRef)
    let message = (envolopeStrong message <| nextMetadata metadata None)
    actorRef.Ask(message, timeout)

let tell (metadata:Metadata) message actorRef = 
    actorRef <! (envolopeStrong message <| nextMetadata metadata None)

let ackMessage (mailbox:Actor<_>) (metadata:Metadata) =
    match metadata.DeliveryId with
    | Some(deliveryId) -> 
        let message = envolopeStrong Ack <| nextMetadata metadata (Some(deliveryId))
        mailbox.Sender() <! message
        Unhandled :> Effect<obj>
    | _ -> Unhandled :> Effect<obj>

type IRecievedMessage =
   abstract member Payload: obj with get

type RecievedMessage<'t> = 
    | Recieved of 't
    override this.ToString() =
        let r = (match this with | Recieved r -> r)
        "Recieved: " + r.ToString()
    interface IRecievedMessage with
        member this.Payload with get() = match this with | Recieved r -> r :> obj

let delivererReceive (log:ILogger) (deliverer:AtLeastOnceDeliverySemantic) (mailbox:Eventsourced<_>) (metadata:Metadata) (message:obj) persist =
    let effect = 
        match message with
        // Hacking this to trigger it all the time, so suppressing it here
        | :? PersistentLifecycleEvent -> Unhandled :> Effect<_>
        | :? Acknowledgement ->
            persist(message) :> Effect<_>
        // Ack a RecievedMessage
        | :? IRecievedMessage as recieved ->
            match recieved.Payload with 
            // If the item is already confirmed Confirm returns false which is fine
            | :? Acknowledgement as ack -> 
                log.Debug("Delivery confirmed for id {deliveryId}, actor {Actor}", metadata.DeliveryId.Value)
                deliverer.Confirm (metadata.DeliveryId.Value) |> ignored
            | _ ->
                if not <| mailbox.IsRecovering() then
                    match metadata.DeliveryId with
                    | Some(deliveryId) ->
                        log.Debug("Acknowledging delivery {deliveryId}, actor {Actor}", deliveryId)
                        ackMessage mailbox metadata
                    | _ -> Unhandled :> Effect<_>
                else 
                    Unhandled :> Effect<_>
        // Returns an effect detailing if the message has already been handled
        | _ -> deliverer.Receive (upcast mailbox) message
    effect


// module Actors

type ActorResult<'t> = 
    | Continue of 't
    | Effect of Effect<obj>

type Context = 
    {
        Metadata: Metadata;
        Mailbox: Actor<obj>;
    }

type ActorLogEnricher (metadata:Metadata) =
    interface ILogEventEnricher with
        member this.Enrich(event:Events.LogEvent, propertyFactory:Core.ILogEventPropertyFactory) = 
            event.AddPropertyIfAbsent(propertyFactory.CreateProperty("MessageId", metadata.MessageId))
            event.AddPropertyIfAbsent(propertyFactory.CreateProperty("ConversationId", metadata.ConversationId))
            event.AddPropertyIfAbsent(propertyFactory.CreateProperty("DeliveryId", metadata.DeliveryId))
            event.AddPropertyIfAbsent(propertyFactory.CreateProperty("Timestamp", metadata.Timestamp))

let createActorBase (log:ILogger) (actorFunc:'state -> Metadata -> 't -> ActorResult<'state>) (initialData:'state) (mailbox:Actor<obj>) = 
    let rec loop state = 
        actor {
            let! (anyMessage:obj) = mailbox.Receive()

            let newMetadata = nextMetadata (metadata ()) None 

            //let thing = typeof<'t>
            //printfn "Type: %O" thing

            let envolopedMessage = 
                match anyMessage with
                | :? Envolope<'t> as exactMatch -> 
                    Some(exactMatch :> IEnvolope)
                | :? IEnvolope as envolope ->
                    match envolope.Payload with
                    | :? 't as exactMatch ->
                        Some(envolope)
                    | _ -> None
                | :? 't as matching ->
                    Some(envolopeStrong matching newMetadata :> IEnvolope)
                | _ -> None

            let resultingEffect =
                match envolopedMessage with
                | Some(message) ->
                    let log = log.ForContext(ActorLogEnricher message.Metadata)
        
                    // Quite mode
                    match anyMessage with
                    | :? LifecycleEvent -> ()
                    | :? RecoveryCompleted -> ()
                    | :? Akka.Persistence.AtLeastOnceDeliverySemantic.RedeliveryTick -> ()
                    | _ -> log.Debug("Base actor {ActorName} handling {Message}", mailbox.Self.Path.Name, anyMessage)

                    let result = actorFunc state message.Metadata (message.Payload :?> _)
                    match result with
                    | Continue(state) -> loop state
                    | Effect(effect) -> effect
                | _ -> 
                    match anyMessage with
                    | :? LifecycleEvent -> Ignore :> _
                    | _ -> Unhandled :> _

            return! resultingEffect

        }
    loop initialData

let createActor log actorFunc initialData mailbox = 
    createActorBase log actorFunc initialData mailbox

let createReliableActor log actorFunc initialData (mailbox:Eventsourced<obj>) =
    let deliverer = createDeliver mailbox
    createActorBase log (fun state metadata message -> 

        let deliver actorPath message =
            deliver deliverer (mailbox.IsRecovering()) actorPath metadata message
            
        let persist message =
            let recievedPayload = Recieved(message)
            let envolopeToSend = envolopeStrong recievedPayload metadata
            PersistentEffect<obj>.Persist(envolopeToSend)

        let effect = delivererReceive log deliverer mailbox metadata message persist :> Effect<_>

        if effect.WasHandled() then 
            Effect(effect)
        else
            //| SnapshotOffer snap ->
            //    deliverer.DeliverySnapshot <- snap

            actorFunc persist deliver state metadata message

    ) initialData mailbox

// Real Actors v2

let crashUntil (name:string) limit = 
    let mutable count = 0
    let crashUntilInner (log:ILogger) =
        count <- count + 1
        if count < limit then
            log.Information("Crashing! {Name}", name)
            failwith "Eeep"
        else ()
    crashUntilInner

// Persisted Reliable Actor That just forwards messages

let reliableActor log (nextActor:Akka.Actor.IActorRef) = 
    propsPersist(createReliableActor log (fun persist deliver state metadata message -> 
        match box message with
        | :? IRecievedMessage as recieved ->
            deliver nextActor recieved.Payload |> ignore // Should throw if false
            Continue(state)
        | :? LifecycleEvent -> Effect(Ignore)
        | :? RecoveryCompleted -> Effect(Ignore)
        | _ ->
            Effect(persist(message))
    ) ())

type Payload = obj
type EventType = EventType of string
type IActorRef = Akka.Actor.IActorRef

type EventBusCommand =
    | Publish of Payload
    | Subscribe of EventType * IActorRef
    | Unsubscribe of EventType * IActorRef

type Subscribers = 
    {
        EventHandlers: Map<EventType, IActorRef list>;
    }

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

type Done = Done

let eventBusActor log = 
    propsPersist((fun mailbox -> 
        createReliableActor log (fun persist deliver state metadata message -> 
            match box message with
            | :? RecievedMessage<EventBusCommand> as recieved ->
                let (Recieved command) = recieved
                match command with
                | Publish(payload) ->

                    let typeName = EventType(payload.GetType().FullName)
                    let delivererFailed = 
                        match state.EventHandlers |> Map.tryFind typeName with
                        | Some actors -> 
                            actors |> Seq.exists (fun actor -> not <| (deliver actor payload) )
                        | _ -> false
                        
                    if (delivererFailed) then
                        raise <| MaxUnconfirmedMessagesExceededException("The deliverer failed to send a message")
                        
                    Continue(state)
                    
                | Subscribe(eventType, actor) ->
                    if not <| mailbox.IsRecovering () then mailbox.Sender() <! Done
                    Continue({ EventHandlers = state.EventHandlers |> addItemToMapList eventType actor })

                | Unsubscribe(eventType, actor) ->
                    if not <| mailbox.IsRecovering () then mailbox.Sender() <! Done
                    Continue({ EventHandlers = state.EventHandlers |> removeItemFromMapList eventType actor })

            | :? EventBusCommand as command ->
                Effect(persist(command))
            | _ -> 
                Effect(Unhandled)
        ) { EventHandlers = Map.empty } mailbox
    ))

let someEventListener (log:ILogger) name crasher = 
    props(fun mailbox ->
        createActor log (fun state metadata message ->

            match box message with
            | :? LifecycleEvent ->
                Effect(Ignore)
            | _ ->
                log.Information("Actor {ActorName} got the message {@Message}", name, message)
                
                crasher log
                ackMessage mailbox metadata |> ignore
                Continue ()
        ) () mailbox)

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
            .WriteTo.ColoredConsole() //Events.LogEventLevel.Debug, "{Timestamp:yyyy-MM-dd HH:mm:ss} [{Level}] {Message}{NewLine}{Exception}")
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

    let someEventListenerRef1 = spawn system "listenerA" <| someEventListener log "A" crasherA
    let someEventListenerRef2 = spawn system "listenerB" <| someEventListener log "B" crasherB

    let eventBusRef = spawn system "eventBus"  <| eventBusActor log
    let eventBusRef : IActorRef<Envolope<EventBusCommand>> = eventBusRef |> retype

    let reliableActorRef = spawn system "deliverer" <| reliableActor log eventBusRef |> retype


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
