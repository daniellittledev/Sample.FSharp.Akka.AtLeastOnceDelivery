// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

open System
open Akka.Persistence
open Akkling
open Akkling.Persistence
open Serilog
open Akka.Event

let crashUntil (name:string) limit = 
    let mutable count = 0
    let crashUntilInner (log:ILogger) =
        count <- count + 1
        if count < limit then
            log.Information("Crashing! {Name}", name)
            failwith "Eeep"
        else ()
    crashUntilInner

type Payload = obj

type DeliveryId = int64

type EventType = EventType of string

type IActorRef = Akka.Actor.IActorRef

type EventBusCommand =
    | Publish of Payload
    | Confirm of DeliveryId
    | Subscribe of EventType * IActorRef
    | Unsubscribe of EventType * IActorRef

type EventBusEvent =
    | Published of Payload
    | Confirmed of DeliveryId
    | Subscribed of EventType * IActorRef
    | Unsubscribed of EventType * IActorRef

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

let delivererReceive (deliverer:AtLeastOnceDeliverySemantic) mailbox (message:obj) =
    let effect = 
        match message with
        // Hacking this to trigger it all the time, so suppressing it here
        | :? PersistentLifecycleEvent -> Unhandled :> Effect<obj>
        | _ -> ( deliverer.Receive (upcast mailbox) message )
    effect

let eventBusActor (log:ILogger) = (propsPersist(fun mailbox ->
    let deliverer = AtLeastOnceDelivery.createDefault mailbox
    deliverer.Receive (upcast mailbox) (upcast ReplaySucceed) |> ignore

    let rec loop state = 
        actor {
            let! msg = mailbox.Receive()

            // Let the AtLeastOnceDeliverySemantic have a go at the message
            let effect = delivererReceive deliverer mailbox msg

            // The AtLeastOnceDelivery Receive may have already handled the message, such as in the case of recovery
            if effect.WasHandled() then return effect
            else
                match msg with

                // This is currently always going to be null as we're not creating any snapshots
                | SnapshotOffer snap ->
                    deliverer.DeliverySnapshot <- snap

                | :? EventBusCommand as cmd ->
                    match cmd with
                    | Publish(payload) ->
                        return Persist(upcast Published(payload))
                    | Confirm(deliveryId) ->
                        return Persist(upcast Confirmed(deliveryId))
                    | Subscribe(eventType, actor) ->
                        return Persist(upcast Subscribed(eventType, actor))
                    | Unsubscribe(eventType, actor) ->
                        return Persist(upcast Unsubscribed(eventType, actor))

                | :? EventBusEvent as evt ->
                    match evt with
                    | Published(payload) ->
                        let map d = { Payload = payload; DeliveryId = d }

                        let typeName = EventType(payload.GetType().FullName)
                        let delivererFailed = 
                            match state.EventHandlers |> Map.tryFind typeName with
                            | Some actors -> 
                                actors |> Seq.exists (fun actor -> not <| deliverer.Deliver(actor.Path, map, mailbox.IsRecovering()))
                            | _ -> false
                        
                        if (delivererFailed) then
                            raise <| MaxUnconfirmedMessagesExceededException("The deliverer failed to send a message")
                        
                        return! loop state
                    | Confirmed(deliveryId) ->
                        // If the item is already confirmed Confirm returns false which is fine
                        return deliverer.Confirm deliveryId |> ignored
                    
                    | Subscribed(eventType, actor) ->
                        if not <| mailbox.IsRecovering () then mailbox.Sender() <! Done
                        return! loop { EventHandlers = state.EventHandlers |> addItemToMapList eventType actor }
                    | Unsubscribed(eventType, actor) ->
                        if not <| mailbox.IsRecovering () then mailbox.Sender() <! Done
                        return! loop { EventHandlers = state.EventHandlers |> removeItemFromMapList eventType actor }

                | _ -> return Unhandled
        }
    loop { EventHandlers = Map.empty }))


let someEventListener (log:ILogger) name crasher = props(fun (mailbox:Actor<_>) ->
    let rec loop () = actor {
        let! (delivery:Delivery) = mailbox.Receive ()
        log.Information("Actor {ActorName} got the message {@Message}", name, delivery)
        crasher log
        mailbox.Sender() <! Confirm(delivery.DeliveryId)
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

    let timeout = Some <| TimeSpan.FromSeconds(1.0)

    spawnAnonymous system <| deadLetterAwareActor log |> ignore

    let crasherA = crashUntil "A" 2
    let crasherB = crashUntil "B" 2

    let someEventListenerRef1 = spawnAnonymous system <| someEventListener log "A" crasherA
    let someEventListenerRef2 = spawnAnonymous system <| someEventListener log "B" crasherB

    let eventBusRef = spawnAnonymous system <| eventBusActor log
    let eventBusRef : IActorRef<EventBusCommand> = eventBusRef |> retype

    let mainAsync = async {

        do! eventBusRef.Ask(Subscribe(EventType(typeof<int32>.FullName), someEventListenerRef1), timeout) |> Async.Ignore
        do! eventBusRef.Ask(Subscribe(EventType(typeof<int32>.FullName), someEventListenerRef2), timeout) |> Async.Ignore

        // Do i need a message Id?

        let message = box 117
    
        eventBusRef <! Publish(message)

    }

    mainAsync |> Async.RunSynchronously

    Console.Read()
    0 // return an integer exit code
