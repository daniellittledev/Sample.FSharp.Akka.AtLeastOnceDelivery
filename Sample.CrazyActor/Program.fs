open Serilog
open Akkling

// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

type ResultEffect<'Message> (value) =
    member val Value = value with get
    interface Effect<'Message> with
        member this.WasHandled () =
            false
        member this.OnApplied(context : ExtActor<'Message>, message : 'Message) = 
            ()

let getMessage (something:obj) = 
    (something :?> ResultEffect<obj>).Value

let myActor = (props(fun mailbox ->
    printfn "Actor started"
    let rec loop state = 
        actor {

            let! something = actor {

                let! anyMessage = mailbox.Receive()
                printfn "Got a message"

                return ResultEffect(anyMessage)
            }

            let value = getMessage something
            
            printfn "Value was: %O" value

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
