module Flow = Conduit_mirage.Flow
module Channel = Mirage_channel_lwt.Make(Flow)

type socket =
  { ic : Channel.t
  ; oc : Channel.t }

module Log =
struct
  let src = Logs.Src.create "git.mirage.net" ~doc:"logs git's mirage net event"
  include (val Logs.src_log src: Logs.LOG)
end

let close socket =
  let open Lwt.Infix in

  let safe_close c =
    Channel.close c >>= function
    | Ok ()
    | Error `Closed -> Lwt.return ()
    | Error e ->
      Log.debug (fun l -> l "Ignoring error (close): %a" Channel.pp_write_error e);
      Lwt.return ()
  in
  safe_close socket.ic >>= fun () ->
  safe_close socket.oc

let write { oc; _ } raw off len =
  Channel.write_string oc (Bytes.unsafe_to_string raw) off len;
  Lwt.return len

let read { ic; _ } raw off len =
  let open Lwt.Infix in

  Channel.read_some ~len ic >>= function
  | Ok `Eof -> Lwt.return 0
  | Error e ->
    Log.debug (fun l -> l "Ignoring error (read): %a" Channel.pp_error e);
    Lwt.return 0
  | Ok (`Data cs) ->
    let len' = Cstruct.len cs in
    Cstruct.blit_to_bytes cs 0 raw off len';
    Lwt.return len'

let resolver = Resolver_mirage.localhost
let conduit = Conduit_mirage.empty

let socket host port =
  let uri =
    Uri.empty
    |> fun uri -> Uri.with_host uri (Some host)
    |> fun uri -> Uri.with_port uri (Some port)
  in

  let open Lwt.Infix in

  Resolver_lwt.resolve_uri ~uri resolver >>= fun endp ->
  Conduit_mirage.client endp >>= fun client ->
  Conduit_mirage.connect conduit client >>= fun flow ->
  let ic = Channel.create flow in
  let oc = Channel.create flow in

  Lwt.return { ic; oc; }
