open Serilog
open Akkling

// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

type StupidEffect<'Message> =
    | ResultEffect of string
    interface Effect<'Message> with
        member this.WasHandled () =
            false
        member this.OnApplied(context : ExtActor<'Message>, message : 'Message) = 
            ()


let myActor = (props(fun mailbox ->
    printfn "Actor started"
    let rec loop state = 
        actor {
            //let! (anyMessage:obj) = mailbox.Receive()
            printfn "Start of loop"
            let! x = Ignore

            let! something = actor {
                //let! something = actor {
                    let! anyMessage = mailbox.Receive()
                    printfn "Got a message"

                    // do some work
                    return ResultEffect("Hello World!")
                //}
                //return something
            }

            match something with
            | :? StupidEffect<obj> as x -> 
                let (ResultEffect value) = x
                printfn "Value was: %s" value

            return loop ()
        }
    loop () ))

[<EntryPoint>]
let main argv = 
    printfn "%A" argv

    let configuration = Configuration.defaultConfig ()
    let system = System.create "system" configuration

    let aref = spawn system "Actor1" myActor
    aref <! ("NopeRope" :> obj)


    system.WhenTerminated |> Async.AwaitTask |> Async.RunSynchronously
    0 // return an integer exit code
