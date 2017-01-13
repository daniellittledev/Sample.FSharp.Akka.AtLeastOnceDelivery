open System

// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

type ActorFunction = (Mailbox -> Effect)
and  LoopFunction = (unit -> Effect)

and Effect =
    | ContinueWith of LoopFunction
    | Stop

and Mailbox (actor:UntypedActor) =
    
    member this.Recieve() =
        actor.GetCurrentMessage()

and UntypedActor (actorFunc:ActorFunction) as this =
    let mailbox = Mailbox(this)
    let mutable messageStore : obj = null
    let mutable behavior = actorFunc mailbox
    member this.Handle(message) =
        messageStore <- message
        match behavior with
        | ContinueWith cont ->
            behavior <- cont ()
        | _ -> ()
    member this.GetCurrentMessage() =
        messageStore

[<EntryPoint>]
let main argv = 
    printfn "%A" argv

    let actor (mailbox:Mailbox) =
        let rec loop state =
            let message = mailbox.Recieve()
            printfn "%i Message: %O" state message
            ContinueWith(fun () -> loop 2)

        ContinueWith(fun () -> loop 1)

    let actorRef = UntypedActor actor
    
    actorRef.Handle("Hello")
    actorRef.Handle("World")

    Console.ReadLine()
    0 // return an integer exit code

// version 2 mailbox -> Outcome[unhandled\ignore\stop\state]