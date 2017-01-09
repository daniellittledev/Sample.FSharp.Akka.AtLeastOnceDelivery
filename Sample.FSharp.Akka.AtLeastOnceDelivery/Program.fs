// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

open System
open Akka.Persistence
open Akkling
open Akkling.Persistence

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
        EventHandlers: Map<EventType, IActorRef>;
    }
    

let messenger = (propsPersist(fun mailbox ->
    let deliverer = AtLeastOnceDelivery.createDefault mailbox
    let rec loop state = 
        actor {
            let! msg = mailbox.Receive()

            // Let the AtLeastOnceDeliverySemantic have a go at the message
            let effect = deliverer.Receive (upcast mailbox) msg
            
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
                        return deliverer.Deliver(recipient.Path, map, mailbox.IsRecovering()) |> ignored
                    | Confirmed(deliveryId) ->
                        return deliverer.Confirm deliveryId |> ignored

                | _ -> return Unhandled
        }
    loop { EventHandlers = Map.empty })) 

[<EntryPoint>]
let main argv = 
    printfn "%A" argv
    0 // return an integer exit code
