// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

type IO<'msg> = | Input

type Cont<'m> =
    | Func of ('m -> Cont<'m>)

module Cont =
    let raw (Func f) = f
    let eff f m = m |> raw f

type ActorBuilder() =
    member this.Bind(m : IO<'msg>, f :'msg -> _) = 
        Func (fun m -> f m)
    member this.ReturnFrom(x) = x
    
    member this.Zero() = fun () -> ()

let actor = ActorBuilder()

type FunActor<'m>(actor: IO<'m> -> Cont<'m>) =
    
    let mutable state = actor Input

    override x.OnReceive(msg) =
        let message = msg :?> 'm
        state <- Cont.eff state message


let system = Actor.system "Actors"


type Message =
    | Inc of int
    | Dec of int
    | Stop

let actor () =
    let rec loop state =
        actor {
            let! msg = recv
            printfn "%d" s
            match msg with
            | Inc n ->
                    return! loop (s + n)
            | Dec n -> 
                return! loop (s - n)
            | Stop -> return! stop ()

            return loop()
        }
        
    loop ()


[<EntryPoint>]
let main argv = 
    printfn "%A" argv
    0 // return an integer exit code
