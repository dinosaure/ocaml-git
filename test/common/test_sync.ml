(*
 * Copyright (c) 2013-2017 Thomas Gazagnaire <thomas@gazagnaire.org>
 * and Romain Calascibetta <romain.calascibetta@gmail.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

open Lwt.Infix

module type SYNC = sig
  (* common ground between Sync and Http.Sync *)
  module Store : Test_store.S

  type error
  type endpoint

  val pp_error : error Fmt.t
  val endpoint_of_uri : Uri.t -> endpoint

  val clone :
       Store.t
    -> reference:Store.Reference.t
    -> endpoint
    -> (unit, error) result Lwt.t

  val fetch_all :
       Store.t
    -> references:Store.Reference.t list Store.Reference.Map.t
    -> endpoint
    -> (unit, error) result Lwt.t

  val update :
       Store.t
    -> reference:Store.Reference.t
    -> endpoint
    -> ( (Store.Reference.t, Store.Reference.t * string) result list
       , error )
       result
       Lwt.t
end

module Make (Sync : SYNC) = struct
  module Store = Sync.Store
  module T = Test_store.Make (Store)

  let root = Fpath.v "test-git-store"

  let run name tests : unit Alcotest.test =
    name, List.map (fun (msg, f) -> msg, `Slow, fun () -> T.run f) tests

  let test_fetch name uris =
    let test endpoint =
      T.create ~root ()
      >>= fun t ->
      (* XXX(dinosaure): an empty repository has the HEAD reference points to
         refs/heads/master which one does not exists. If we want to know if a
         repository is empty, we need to check [Store.Reference.master]. *)
      Store.Ref.mem t Store.Reference.master
      >>= function
      | true -> Alcotest.fail "non-empty repository!"
      | false -> (
          let references =
            let open Store.Reference in
            Map.add master [master] Map.empty
          in
          Sync.fetch_all t ~references endpoint
          >>= function
          | Error err -> Alcotest.failf "%a" Sync.pp_error err
          | Ok () -> (
              Sync.update t ~reference:Store.Reference.master endpoint
              >|= function
              | Error err -> Alcotest.failf "%a" Sync.pp_error err
              | Ok [] -> ()
              | Ok (_ :: _) ->
                  Alcotest.fail "de-synchronization of the git repository" ) )
    in
    let tests =
      List.map
        (fun uri ->
          let msg = Fmt.strf "fetching" in
          msg, fun () -> test (Sync.endpoint_of_uri uri) )
        uris
    in
    run name tests

  let test_clone name uris =
    let test endpoint reference =
      Store.v root
      >>= T.check_err
      >>= fun t ->
      Sync.clone t ~reference endpoint
      >>= fun _ ->
      Store.list t
      >>= Lwt_list.iter_s (fun hash -> Store.read_exn t hash >|= fun _ -> ())
      >>= fun () ->
      Store.Ref.read t Store.Reference.head >>= T.check_err >|= fun _ -> ()
    in
    let tests =
      List.map
        (fun (uri, reference) ->
          let reference = Store.Reference.P.(heads / reference) in
          let msg =
            Fmt.strf "cloning (branch=%a)" Store.Reference.pp reference
          in
          msg, fun () -> test (Sync.endpoint_of_uri uri) reference )
        uris
    in
    run name tests
end
