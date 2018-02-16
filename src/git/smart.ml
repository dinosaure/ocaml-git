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

module type COMMON =
sig
  type hash
  type reference

  type advertised_refs =
    { shallow      : Hash.t list
    ; refs         : (hash * reference * bool) list
    ; capabilities : Capability.t list }

  type shallow_update =
    { shallow   : hash list
    ; unshallow : hash list }

  type acks =
    { shallow   : hash list
    ; unshallow : hash list
    ; acks      : (hash * [ `Common | `Ready | `Continue | `ACK ]) list }

  type negociation_result =
    | NAK
    | ACK of hash
    | ERR of string

  type pack =
    [ `Raw of Cstruct.t
    | `Out of Cstruct.t
    | `Err of Cstruct.t ]

  type report_status =
    { unpack   : (unit, string) result
    ; commands : (reference, reference * string) result list }

  type upload_request =
    { want         : hash * hash list
    ; capabilities : Capability.t list
    ; shallow      : hash list
    ; deep         : [ `Depth of int | `Timestamp of int64 | `Ref of reference ] option }

  type request_command =
    [ `Upload_pack
    | `Receive_pack
    | `Upload_archive ]

  type git_proto_request =
    { pathname        : string
    ; host            : (string * int option) option
    ; request_command : request_command }

  type command =
    | Create of hash * reference
    | Delete of hash * reference
    | Update of hash * hash * reference

  type push_certificate =
    { pusher   : string
    ; pushee   : string
    ; nonce    : string
    ; options  : string list
    ; commands : command list
    ; gpg      : string list }

  type update_request =
    { shallow      : hash list
    ; requests     : [`Raw of command * command list | `Cert of push_certificate]
    ; capabilities : Capability.t list }

  type http_upload_request =
    { want         : hash * hash list
    ; capabilities : Capability.t list
    ; shallow      : hash list
    ; deep         : [ `Depth of int | `Timestamp of int64 | `Ref of reference ] option
    ; has          : hash list }

  val pp_advertised_refs: advertised_refs Fmt.t
  val pp_shallow_update: shallow_update Fmt.t
  val pp_acks: acks Fmt.t
  val pp_negociation_result: negociation_result Fmt.t
  val pp_pack: pack Fmt.t
  val pp_report_status: report_status Fmt.t
  val pp_upload_request: upload_request Fmt.t
  val pp_request_command: request_command Fmt.t
  val pp_git_proto_request: git_proto_request Fmt.t
  val pp_command: command Fmt.t
  val pp_push_certificate: push_certificate Fmt.t
  val pp_update_request: update_request Fmt.t
  val pp_http_upload_request: http_upload_request Fmt.t

  type 'a equal = 'a -> 'a -> bool

  val equal_advertised_refs: advertised_refs equal
  val equal_shallow_update: shallow_update equal
  val equal_acks: acks equal
  val equal_negociation_result: negociation_result equal
  val equal_pack: pack equal
  val equal_report_status: report_status equal
  val equal_upload_request: upload_request equal
  val equal_request_command: request_command equal
  val equal_git_proto_request: git_proto_request equal
  val equal_command: command equal
  val equal_push_certificate: push_certificate equal
  val equal_update_request: update_request equal
  val equal_http_upload_request: http_upload_request equal
end

module Common (Hash: S.HASH) (Reference: Reference.S)
  : COMMON with type hash = Hash.t
            and type reference = Reference.t
= struct
  type hash = Hash.t
  type reference = Reference.t
  type 'a equal = 'a -> 'a -> bool

  type advertised_refs =
    { shallow      : hash list
    ; refs         : (hash * reference * bool) list
    ; capabilities : Capability.t list }

  let pp_advertised_refs ppf { shallow; refs; capabilities; } =
    let pp_ref ppf (hash, reference, peeled) =
      match peeled with
      | true -> Fmt.pf ppf "%a^{}:%a" Reference.pp reference Hash.pp hash
      | false -> Fmt.pf ppf "%a:%a" Reference.pp reference Hash.pp hash in
    Fmt.pf ppf "{ @[<hov>shallow = %a;@ refs = %a;@ capabilities = %a;@]}"
      Fmt.Dump.(list Hash.pp) shallow
      Fmt.Dump.(list pp_ref) refs
      Fmt.Dump.(list Capability.pp) capabilities

  let equal_advertised_refs a b =
    let equal_ref (hash_a, ref_a, peeled_a) (hash_b, ref_b, peeled_b) =
      Hash.equal hash_a hash_b
      && Reference.equal ref_a ref_b
      && peeled_a = peeled_b in
    let compare_ref (_, a, _) (_, b, _) = Reference.compare a b in
    try
      List.for_all2 Hash.equal
        (List.sort Hash.compare a.shallow)
        (List.sort Hash.compare b.shallow)
      && List.for_all2 equal_ref
        (List.sort compare_ref a.refs)
        (List.sort compare_ref b.refs)
      && List.for_all2 Capability.equal
        (List.sort Capability.compare a.capabilities)
        (List.sort Capability.compare b.capabilities)
    with Invalid_argument _ -> false

  type shallow_update =
    { shallow   : hash list
    ; unshallow : hash list }

  let pp_shallow_update ppf { shallow; unshallow; } =
    Fmt.pf ppf "{ @[<hov>shallow = %a;@ unshallow = %a;@] }"
      Fmt.Dump.(list Hash.pp) shallow
      Fmt.Dump.(list Hash.pp) unshallow

  let equal_shallow_update a b =
    try
      List.for_all2 Hash.equal
        (List.sort Hash.compare a.shallow)
        (List.sort Hash.compare b.shallow)
      && List.for_all2 Hash.equal
        (List.sort Hash.compare a.unshallow)
        (List.sort Hash.compare b.unshallow)
    with Invalid_argument _ -> false

  type acks =
    { shallow   : hash list
    ; unshallow : hash list
    ; acks      : (hash * [ `Common | `Ready | `Continue | `ACK ]) list }

  let pp_acks ppf { shallow; unshallow; acks; } =
    let pp_ack ppf (hash, ack) = match ack with
      | `Continue -> Fmt.pf ppf "continue:%a" Hash.pp hash
      | `Ready    -> Fmt.pf ppf "ready:%a" Hash.pp hash
      | `Common   -> Fmt.pf ppf "common:%a" Hash.pp hash
      | `ACK      -> Fmt.pf ppf "ACK:%a" Hash.pp hash in
    Fmt.pf ppf "{ @[<hov>shallow = %a;@ unshallow = %a;@ acks = %a;@] }"
      Fmt.Dump.(list Hash.pp) shallow
      Fmt.Dump.(list Hash.pp) unshallow
      Fmt.Dump.(list pp_ack) acks

  let equal_acks a b =
    let equal_ack (hash_a, detail_a) (hash_b, detail_b) =
      Hash.equal hash_a hash_b
      && (match detail_a, detail_b with
          | `Common, `Common -> true
          | `Ready, `Ready -> true
          | `Continue, `Continue -> true
          | `ACK, `ACK -> true
          | _, _ -> false) in
    let compare_ack (a, _) (b, _) = Hash.compare a b in
    try
      List.for_all2 Hash.equal
        (List.sort Hash.compare a.shallow)
        (List.sort Hash.compare b.shallow)
      && List.for_all2 Hash.equal
        (List.sort Hash.compare a.unshallow)
        (List.sort Hash.compare b.unshallow)
      && List.for_all2 equal_ack
        (List.sort compare_ack a.acks)
        (List.sort compare_ack b.acks)
    with Invalid_argument _ -> false

  type negociation_result =
    | NAK
    | ACK of hash
    | ERR of string

  let pp_negociation_result ppf = function
    | NAK -> Fmt.string ppf "NAK"
    | ACK hash -> Fmt.pf ppf "(ACK %a)" Hash.pp hash
    | ERR err -> Fmt.pf ppf "(ERR %s)" err

  let equal_negociation_result a b = match a, b with
    | NAK, NAK -> true
    | ACK a, ACK b -> Hash.equal a b
    | ERR a, ERR b -> String.equal a b
    | _, _ -> false

  type pack =
    [ `Raw of Cstruct.t
    | `Out of Cstruct.t
    | `Err of Cstruct.t ]

  let pp_pack ppf = function
    | `Raw raw ->
      Fmt.pf ppf "@[<5>(Raw %a)@]"
        (Minienc.pp_scalar ~get:Cstruct.get_char ~length:Cstruct.len) raw
    | `Out out ->
      Fmt.pf ppf "@[<5>(Out %S)@]" (Cstruct.to_string out)
    | `Err err ->
      Fmt.pf ppf "@[<5>(Err %S)@]" (Cstruct.to_string err)

  let equal_pack a b = match a, b with
    | `Err a, `Err b
    | `Out a, `Out b
    | `Raw a, `Raw b -> Cstruct.equal a b
    | _, _ -> false

  type report_status =
    { unpack   : (unit, string) result
    ; commands : (reference, reference * string) result list }

  let pp_report_status ppf { unpack; commands; } =
    Fmt.pf ppf "{ @[<hov>unpack = %a;@ commands = %a;@] }"
      Fmt.(Dump.result ~ok:Fmt.nop ~error:Fmt.string) unpack
      Fmt.(Dump.list (Dump.result ~ok:Reference.pp ~error:(pair Reference.pp string))) commands

  let equal_report_status a b =
    let equal_result ~ok ~error a b = match a, b with
      | Ok a, Ok b -> ok a b
      | Error a, Error b -> error a b
      | _, _ -> false in
    let compare_result ~ok ~error a b = match a, b with
      | Ok a, Ok b -> ok a b
      | Error a, Error b -> error a b
      | Ok _, Error _ -> 1
      | Error _, Ok _ -> (-1) in
    let compare_ref_and_msg (ref_a, msg_a) (ref_b, msg_b) =
      let res = Reference.compare ref_a ref_b in
      if res = 0 then String.compare msg_a msg_b else res in
    let equal_ref_and_msg a b = compare_ref_and_msg a b = 0 in
    try
      equal_result ~ok:(fun () () -> true) ~error:String.equal a.unpack b.unpack
      && List.for_all2 (equal_result ~ok:Reference.equal ~error:equal_ref_and_msg)
        (List.sort (compare_result ~ok:Reference.compare ~error:compare_ref_and_msg) a.commands)
        (List.sort (compare_result ~ok:Reference.compare ~error:compare_ref_and_msg) b.commands)
    with Invalid_argument _ -> false

  type upload_request =
    { want         : hash * hash list
    ; capabilities : Capability.t list
    ; shallow      : hash list
    ; deep         : [ `Depth of int | `Timestamp of int64 | `Ref of reference ] option }

  let pp_upload_request ppf { want; capabilities; shallow; deep; } =
    let pp_deep ppf = function
      | `Depth n -> Fmt.pf ppf "(Depth %d)" n
      | `Timestamp n -> Fmt.pf ppf "(Timestamp %Ld)" n
      | `Ref r -> Fmt.pf ppf "(`Ref %a)" Reference.pp r in
    Fmt.pf ppf "{ @[<hov>want = %a;@ capabiliteis = %a;@ shallow = %a; deep = %a;@] }"
      Fmt.(Dump.list Hash.pp) (fst want :: snd want)
      Fmt.(Dump.list Capability.pp) capabilities
      Fmt.(Dump.list Hash.pp) shallow
      Fmt.(Dump.option pp_deep) deep

  let equal_upload_request a b =
    try
      let equal_deep a b = match a, b with
        | `Depth a, `Depth b -> a = b
        | `Timestamp a, `Timestamp b -> Int64.equal a b
        | `Ref a, `Ref b -> Reference.equal a b
        | _, _ -> false in

      List.for_all2 Hash.equal
        (List.sort Hash.compare (fst a.want :: snd a.want))
        (List.sort Hash.compare (fst b.want :: snd b.want))
      && List.for_all2 Capability.equal
        (List.sort Capability.compare a.capabilities)
        (List.sort Capability.compare b.capabilities)
      && List.for_all2 Hash.equal
        (List.sort Hash.compare a.shallow)
        (List.sort Hash.compare b.shallow)
      && (match a.deep, b.deep with
          | Some a, Some b -> equal_deep a b
          | None, None -> true
          | _, _ -> false)
    with Invalid_argument _ -> false

  type request_command =
    [ `Upload_pack
    | `Receive_pack
    | `Upload_archive ]

  let equal_request_command a b = match a, b with
    | `Upload_pack, `Upload_pack
    | `Receive_pack, `Receive_pack
    | `Upload_archive, `Upload_archive -> true
    | _, _ -> true

  let pp_request_command ppf = function
    | `Upload_pack -> Fmt.string ppf "`Upload_pack"
    | `Receive_pack -> Fmt.string ppf "`Receive_pack"
    | `Upload_archive -> Fmt.string ppf "`Upload_archive"

  type git_proto_request =
    { pathname        : string
    ; host            : (string * int option) option
    ; request_command : request_command }

  let pp_git_proto_request ppf { pathname; host; request_command; } =
    let pp_host ppf (host, port) = match port with
      | Some port -> Fmt.pf ppf "%s:%d" host port
      | None -> Fmt.string ppf host in
    Fmt.pf ppf "{ @[<hov>pathname = %s;@ host = %a;@ request_command = %a;@] }"
      pathname
      Fmt.(Dump.option pp_host) host
      pp_request_command request_command

  let equal_git_proto_request a b =
    let equal_option equal a b = match a, b with
      | Some a, Some b -> equal a b
      | None, None -> true
      | _, _ -> false in
    let equal_host (host_a, port_a) (host_b, port_b) =
      String.equal host_a host_b
      && equal_option (=) port_a port_b in
    String.equal a.pathname b.pathname
    && equal_option equal_host a.host b.host
    && equal_request_command a.request_command b.request_command

  type command =
    | Create of hash * reference
    | Delete of hash * reference
    | Update of hash * hash * reference

  let pp_command ppf = function
    | Create (hash, reference) -> Fmt.pf ppf "(Create %a:%a)" Reference.pp reference Hash.pp hash
    | Delete (hash, reference) -> Fmt.pf ppf "(Delete %a:%a)" Reference.pp reference Hash.pp hash
    | Update (a, b, reference) -> Fmt.pf ppf "(Create %a:@[<0>%a -> %a@])" Reference.pp reference Hash.pp a Hash.pp b

  let equal_command a b = match a, b with
    | Create (hash_a, ref_a), Create (hash_b, ref_b) ->
      Hash.equal hash_a hash_b && Reference.equal ref_a ref_b
    | Delete (hash_a, ref_a), Delete (hash_b, ref_b) ->
      Hash.equal hash_a hash_b && Reference.equal ref_a ref_b
    | Update (to_a, of_a, ref_a), Update (to_b, of_b, ref_b) ->
      Hash.equal to_a to_b && Hash.equal of_a of_b && Reference.equal ref_a ref_b
    | _, _ -> false

  type push_certificate =
    { pusher   : string
    ; pushee   : string
    ; nonce    : string
    ; options  : string list
    ; commands : command list
    ; gpg      : string list }

  let pp_push_certificate ppf pcert =
    Fmt.pf ppf "{ @[<hov>pusher = %s;@ pushee = %s;@ nonce = %S;@ options = %a;@ commands = %a;@ gpg = %a;@] }"
      pcert.pusher pcert.pushee pcert.nonce
      Fmt.(Dump.list string) pcert.options
      Fmt.(Dump.list pp_command) pcert.commands
      Fmt.(Dump.list (fmt "%S")) pcert.gpg

  let compare_command a b = match a, b with
    | Create (hash_a, ref_a), Create (hash_b, ref_b)
    | Delete (hash_a, ref_a), Delete (hash_b, ref_b) ->
      let res = Hash.compare hash_a hash_b in
      if res = 0 then Reference.compare ref_a ref_b else res
    | Update (to_a, of_a, ref_a), Update (to_b, of_b, ref_b) ->
      let res = Hash.compare to_a to_b in
      if res = 0 then
        let res = Hash.compare of_a of_b in
        if res = 0 then Reference.compare ref_a ref_b else res
      else res
    | Create _, _ -> 1
    | Delete _, Create _ -> (-1)
    | Update _, Create _ -> (-1)
    | Delete _, _ -> 1
    | Update _, Delete _ -> (-1)

  let equal_push_certificate a b =
    try
      String.equal a.pusher b.pusher
      && String.equal a.pushee b.pushee
      && String.equal a.nonce b.nonce
      && List.for_all2 String.equal
        (List.sort String.compare a.options)
        (List.sort String.compare b.options)
      && List.for_all2 equal_command
        (List.sort compare_command a.commands)
        (List.sort compare_command b.commands)
    with Invalid_argument _ -> false

  type update_request =
    { shallow      : hash list
    ; requests     : [`Raw of command * command list | `Cert of push_certificate]
    ; capabilities : Capability.t list }

  let pp_update_request ppf { shallow; requests; capabilities; } =
    let pp_requests ppf = function
      | `Raw commands -> Fmt.(Dump.list pp_command) ppf (fst commands :: snd commands)
      | `Cert pcert -> pp_push_certificate ppf pcert in
    Fmt.pf ppf "{ @[<hov>shallow = %a;@ requests = %a;@ capabilities = %a;@] }"
      Fmt.(Dump.list Hash.pp) shallow
      (Fmt.hvbox pp_requests) requests
      Fmt.(Dump.list Capability.pp) capabilities

  let equal_update_request a b =
    let equal_raw a b =
      List.for_all2 equal_command
        (List.sort compare_command (fst a :: snd a))
        (List.sort compare_command (fst b :: snd b)) in
    try
      List.for_all2 Hash.equal
        (List.sort Hash.compare a.shallow)
        (List.sort Hash.compare b.shallow)
      && (match a.requests, b.requests with
          | `Raw a, `Raw b -> equal_raw a b
          | `Cert a, `Cert b -> equal_push_certificate a b
          | _, _ -> false)
      && List.for_all2 Capability.equal
        (List.sort Capability.compare a.capabilities)
        (List.sort Capability.compare b.capabilities)
    with Invalid_argument _ -> false

  type http_upload_request =
    { want         : hash * hash list
    ; capabilities : Capability.t list
    ; shallow      : hash list
    ; deep         : [ `Depth of int | `Timestamp of int64 | `Ref of reference ] option
    ; has          : hash list }

  let equal_http_upload_request a b =
    try
      equal_upload_request
        { want = a.want
        ; capabilities = a.capabilities
        ; shallow = a.shallow
        ; deep = a.deep }
        { want = b.want
        ; capabilities = b.capabilities
        ; shallow = b.shallow
        ; deep = b.deep }
      && List.for_all2 Hash.equal
        (List.sort Hash.compare a.has)
        (List.sort Hash.compare b.has)
    with Invalid_argument _ -> false

  let pp_http_upload_request ppf { want; capabilities; shallow; deep; has; } =
    let pp_deep ppf = function
      | `Depth n -> Fmt.pf ppf "(Depth %d)" n
      | `Timestamp n -> Fmt.pf ppf "(Timestamp %Ld)" n
      | `Ref r -> Fmt.pf ppf "(`Ref %a)" Reference.pp r in
    Fmt.pf ppf "{ @{<hov>want = %a;@ capabilities = %a;@ shallow = %a;@ deep = %a;@ has = %a;@} }"
      Fmt.(Dump.list Hash.pp) (fst want :: snd want)
      Fmt.(Dump.list Capability.pp) capabilities
      Fmt.(Dump.list Hash.pp) shallow
      Fmt.(Dump.option pp_deep) deep
      Fmt.(Dump.list Hash.pp) has
end

module type DECODER =
sig
  module Hash: S.HASH
  module Reference: Reference.S
  module Common: COMMON with type hash := Hash.t and type reference := Reference.t

  type decoder

  val pp_decoder: decoder Fmt.t

  type error =
    [ `Expected_char of char
    | `Unexpected_char of char
    | `Unexpected_flush_pkt_line
    | `No_assert_predicate of (char -> bool)
    | `Expected_string of string
    | `Unexpected_empty_pkt_line
    | `Malformed_pkt_line
    | `Unexpected_end_of_input
    | `Unexpected_pkt_line ]

  val pp_error: error Fmt.t

  type 'a state =
    | Ok of 'a
    | Read of { buffer   : Cstruct.t
              ; off      : int
              ; len      : int
              ; continue : int -> 'a state }
    | Error of { err       : error
               ; buf       : Cstruct.t
               ; committed : int }

  type _ transaction =
    | HttpReferenceDiscovery : string -> Common.advertised_refs transaction
    | ReferenceDiscovery     : Common.advertised_refs transaction
    | ShallowUpdate          : Common.shallow_update transaction
    | Negociation            : Hash.Set.t * ack_mode -> Common.acks transaction
    | NegociationResult      : Common.negociation_result transaction
    | PACK                   : side_band -> flow transaction
    | ReportStatus           : side_band -> Common.report_status transaction
    | HttpReportStatus       : string list * side_band -> Common.report_status transaction
  and ack_mode =
    [ `Ack
    | `Multi_ack
    | `Multi_ack_detailed ]
  and flow =
    [ `Raw of Cstruct.t
    | `End
    | `Err of Cstruct.t
    | `Out of Cstruct.t ]
  and side_band =
    [ `Side_band
    | `Side_band_64k
    | `No_multiplexe ]

  val decode: decoder -> 'result transaction -> 'result state
  val decoder: unit -> decoder
  val of_string: string -> 'v transaction -> ('v, error * Cstruct.t * int) result
end

module type ENCODER =
sig
  module Hash: S.HASH
  module Reference: Reference.S
  module Common: COMMON with type hash := Hash.t and type reference := Reference.t

  type encoder

  val set_pos: encoder -> int -> unit
  val free: encoder -> Cstruct.t

  type 'a state =
    | Write of { buffer    : Cstruct.t
               ; off       : int
               ; len       : int
               ; continue  : int -> 'a state }
    | Ok of 'a

  type action =
    [ `GitProtoRequest    of Common.git_proto_request
    | `UploadRequest      of Common.upload_request
    | `HttpUploadRequest  of [ `Done | `Flush ] * Common.http_upload_request
    | `UpdateRequest      of Common.update_request
    | `HttpUpdateRequest  of Common.update_request
    | `Has                of Hash.Set.t
    | `Done
    | `Flush
    | `Shallow            of Hash.t list
    | `PACK               of int ]

  val encode: encoder -> action -> unit state
  val encoder: unit -> encoder
  val to_string: action -> string
end

module type CLIENT =
sig
  module Hash: S.HASH
  module Reference: Reference.S

  module Common: COMMON
    with type hash := Hash.t
     and type reference := Reference.t
  module Decoder: DECODER
    with module Hash = Hash
     and module Reference = Reference
     and module Common := Common
  module Encoder: ENCODER
    with module Hash = Hash
     and module Reference = Reference
     and module Common := Common

  type context

  type result =
    [ `Refs of Common.advertised_refs
    | `ShallowUpdate of Common.shallow_update
    | `Negociation of Common.acks
    | `NegociationResult of Common.negociation_result
    | `PACK of Decoder.flow
    | `Flush
    | `Nothing
    | `ReadyPACK of Cstruct.t
    | `ReportStatus of Common.report_status ]
  type process =
    [ `Read of (Cstruct.t * int * int * (int -> process))
    | `Write of (Cstruct.t * int * int * (int -> process))
    | `Error of (Decoder.error * Cstruct.t * int)
    | result ]
  type action =
    [ `GitProtoRequest of Common.git_proto_request
    | `Shallow of Hash.t list
    | `UploadRequest of Common.upload_request
    | `UpdateRequest of Common.update_request
    | `Has of Hash.Set.t
    | `Done
    | `Flush
    | `ReceivePACK
    | `SendPACK of int
    | `FinishPACK ]

  val capabilities: context -> Capability.t list
  val set_capabilities: context -> Capability.t list -> unit
  val encode: Encoder.action -> (context -> process) -> context -> process
  val decode: 'a Decoder.transaction -> ('a -> context -> process) -> context -> process
  val pp_result: result Fmt.t
  val run: context -> action -> process
  val context: Common.git_proto_request -> context * process
end

module Decoder
    (Hash: S.HASH)
    (Reference: Reference.S)
    (Common: COMMON with type hash := Hash.t and type reference := Reference.t)
  : DECODER
    with module Hash = Hash
     and module Reference = Reference
     and module Common = Common =
struct
  module Hash = Hash
  module Reference = Reference
  module Common = Common

  (* XXX(dinosaure): Why this decoder? We can use Angstrom instead or another
     library. It's not my first library about the parsing (see Mr. MIME) and I
     like a lot Angstrom. But I know the limitation about Angstrom and the best
     case to use it. I already saw other libraries like ocaml-imap specifically
     to find the best way to parse an email.

     You need all the time to handle the performance, the re-usability, the
     scalability and others constraints like the memory.

     So, about the smart Git protocol, I have the choice between Angstrom,
     something similar than the PACK decoder or this decoder.

     - Angstrom is good to describe the smart Git protocol. The expressivity is
       good and the performance is another good point. A part of Angstrom is
       about the alteration when you have some possibilities about the input. We
       have some examples when we compute the format of the Git object.

       And the best point is to avoid any headache to handle the input buffer
       about any alteration. I explained this specific point in the [Helper]
       module (which one provide a common non-blocking interface to decode
       something described by Angstrom).

       For all of this, it's a good way to use Angstrom in this case. But it's
       not the best. Indeed, the smart Git protocol is think in all state about
       the length of the input by the /pkt-line/ format. Which one describes all
       the time the length of the payload and limit this payload to 65520 bytes.

       So the big constraint about the alteration and when we need to keep some
       bytes in the current input buffer to retry the next alteration if the
       first one fails (and have a headache to handle the input) never happens.
       And if it's happen, the input is wrong.

     - like the PACK Decoder. If you look the PACK Decoder, it's another way to
       decode something in the non-blocking world. The good point is to handle
       all aspect of your decoder and, sometimes, describe a weird semantic
       about your decoder which is not available in Angstrom. You can do
       something hacky and wrap all in a good interface « à la Daniel Bünzli ».

       So if you want to do something fast and hacky in some contexts (like
       switch between a common functional way and a imperative way easily)
       because you know the constraint about your protocol/format, it's a good
       way. But you need a long time to do this and it is not easily composable
       like Angstrom because it's closely specific to your protocol/format.

     - like ocaml-imap. The IMAP protocol is very close to the smart Git
       protocol in some way and the interface seems to be good to have an
       user-friendly interface to communicate with a Git server without a big
       overhead because the decoder is funded on some assertions about the
       protocol (like the PKT line for the smart Git protocol or the end of line
       for the IMAP protocol).

       Then, the decoder is very hacky because we don't use the continuation all
       the time (like Angstrom) to keep a complex state but just fuck all up by
       an exception.

       And the composition between some conveniences primitives is easy (more
       easy than the second way).

     So for all of this, I decide to use this way to decode the smart Git
     protocol and provide a clear interface to the user (and keep a non-blocking
     land about all). So enjoy it!
  *)

  module Log =
  struct
    let src = Logs.Src.create "git.smart.decoder" ~doc:"logs git's smart decoder event"
    include (val Logs.src_log src : Logs.LOG)
  end

  type decoder =
    { mutable buffer : Cstruct.t
    ; mutable pos    : int
    ; mutable eop    : int option (* end of packet *)
    ; mutable max    : int }

  let pp_decoder ppf { buffer; pos; eop; max; } =
    let pp = Minienc.pp_scalar ~get:Cstruct.get_char ~length:Cstruct.len in

    match eop with
    | Some eop ->
      Fmt.pf ppf "{ @[<hov>current = %a;@ \
                  next = %a;@] }"
        (Fmt.hvbox pp) (Cstruct.sub buffer pos eop)
        (Fmt.hvbox pp) (Cstruct.sub buffer eop max)
    | None -> Fmt.pf ppf "#raw"

  type error =
    [ `Expected_char of char
    | `Unexpected_char of char
    | `Unexpected_flush_pkt_line
    | `No_assert_predicate of (char -> bool)
    | `Expected_string of string
    | `Unexpected_empty_pkt_line
    | `Malformed_pkt_line
    | `Unexpected_end_of_input
    | `Unexpected_pkt_line ]

  let err_unexpected_end_of_input    decoder = (`Unexpected_end_of_input, decoder.buffer, decoder.pos)
  let err_expected               chr decoder = (`Expected_char chr, decoder.buffer, decoder.pos)
  let err_unexpected_char        chr decoder = (`Unexpected_char chr, decoder.buffer, decoder.pos)
  let err_assert_predicate predicate decoder = (`No_assert_predicate predicate, decoder.buffer, decoder.pos)
  let err_expected_string          s decoder = (`Expected_string s, decoder.buffer, decoder.pos)
  let err_unexpected_empty_pkt_line  decoder = (`Unexpected_empty_pkt_line, decoder.buffer, decoder.pos)
  let err_malformed_pkt_line         decoder = (`Malformed_pkt_line, decoder.buffer, decoder.pos)
  let err_unexpected_flush_pkt_line  decoder = (`Unexpected_flush_pkt_line, decoder.buffer, decoder.pos)
  let err_unexpected_pkt_line        decoder = (`Unexpected_pkt_line, decoder.buffer, decoder.pos)

  let pp_error ppf = function
    | `Expected_char chr         -> Fmt.pf ppf "(`Expected_char %c)" chr
    | `Unexpected_char chr       -> Fmt.pf ppf "(`Unexpected_char %c)" chr
    | `No_assert_predicate _     -> Fmt.pf ppf "(`No_assert_predicate #predicate)"
    | `Expected_string s         -> Fmt.pf ppf "(`Expected_string %s)" s
    | `Unexpected_empty_pkt_line -> Fmt.pf ppf "`Unexpected_empty_pkt_line"
    | `Malformed_pkt_line        -> Fmt.pf ppf "`Malformed_pkt_line"
    | `Unexpected_end_of_input   -> Fmt.pf ppf "`Unexpected_end_of_input"
    | `Unexpected_flush_pkt_line -> Fmt.pf ppf "`Unexpected_flush_pkt_line"
    | `Unexpected_pkt_line       -> Fmt.pf ppf "`Unexpected_pkt_line"

  type 'a state =
    | Ok of 'a
    | Read of { buffer     : Cstruct.t
              ; off        : int
              ; len        : int
              ; continue   : int -> 'a state }
    | Error of { err       : error
               ; buf       : Cstruct.t
               ; committed : int }

  exception Leave of (error * Cstruct.t * int)

  let p_return (type a) (x : a) _ : a state = Ok x

  let p_safe k decoder : 'a state =
    try k decoder
    with Leave (err, buf, pos) ->
      Error { err
            ; buf
            ; committed = pos }

  let p_end_of_input decoder = match decoder.eop with
    | Some eop -> eop
    | None -> decoder.max

  let p_peek_char decoder =
    if decoder.pos < (p_end_of_input decoder)
    then Some (Cstruct.get_char decoder.buffer decoder.pos)
    else None

  let p_current decoder =
    if decoder.pos < (p_end_of_input decoder)
    then Cstruct.get_char decoder.buffer decoder.pos
    else raise (Leave (err_unexpected_end_of_input decoder))

  let p_junk_char decoder =
    if decoder.pos < (p_end_of_input decoder)
    then decoder.pos <- decoder.pos + 1
    else raise (Leave (err_unexpected_end_of_input decoder))

  let p_char chr decoder =
    match p_peek_char decoder with
    | Some chr' when chr' = chr ->
      p_junk_char decoder
    | Some _ ->
      raise (Leave (err_expected chr decoder))
    | None ->
      raise (Leave (err_unexpected_end_of_input decoder))

  let p_satisfy predicate decoder =
    match p_peek_char decoder with
    | Some chr when predicate chr ->
      p_junk_char decoder; chr
    | Some _ ->
      raise (Leave (err_assert_predicate predicate decoder))
    | None ->
      raise (Leave (err_unexpected_end_of_input decoder))

  let p_space decoder = p_char ' ' decoder
  let p_null  decoder = p_char '\000' decoder

  let p_while1 predicate decoder =
    let i0 = decoder.pos in

    while decoder.pos < (p_end_of_input decoder)
          && predicate (Cstruct.get_char decoder.buffer decoder.pos)
    do decoder.pos <- decoder.pos + 1 done;

    if i0 < decoder.pos
    then Cstruct.sub decoder.buffer i0 (decoder.pos - i0)
    else raise (Leave (err_unexpected_char (p_current decoder) decoder))

  let p_while0 predicate decoder =
    let i0 = decoder.pos in

    while decoder.pos < (p_end_of_input decoder)
          && predicate (Cstruct.get_char decoder.buffer decoder.pos)
    do decoder.pos <- decoder.pos + 1 done;

    Cstruct.sub decoder.buffer i0 (decoder.pos - i0)

  let p_string s decoder =
    let i0 = decoder.pos in
    let ln = String.length s in

    while decoder.pos < (p_end_of_input decoder)
          && (decoder.pos - i0) < ln
          && String.get s (decoder.pos - i0) = Cstruct.get_char decoder.buffer decoder.pos
    do decoder.pos <- decoder.pos + 1 done;

    if decoder.pos - i0 = ln
    then Cstruct.sub decoder.buffer i0 ln
    else raise (Leave (err_expected_string s decoder))

  let p_hexdigit decoder =
    match p_satisfy (function '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' -> true | _ -> false) decoder with
    | '0' .. '9' as chr -> Char.code chr - 48
    | 'a' .. 'f' as chr -> Char.code chr - 87
    | 'A' .. 'F' as chr -> Char.code chr - 55
    | _ -> assert false

  let p_pkt_payload ?(strict = false) k decoder expect =
    let pkt = if expect < 0 then `Malformed else if expect = 0 then `Empty else `Line expect in

    if expect <= 0
    then begin
      decoder.eop <- Some decoder.pos;
      k ~pkt decoder
    end else begin
      (* compress *)
      if decoder.pos > 0
      then begin
        Cstruct.blit decoder.buffer decoder.pos decoder.buffer 0 (decoder.max - decoder.pos);
        decoder.max <- decoder.max - decoder.pos;
        decoder.pos <- 0;
      end;

      let rec loop rest off =
        if rest <= 0
        then begin
          let off, pkt =
            if Cstruct.get_char decoder.buffer (off + rest - 1) = '\n' && not strict
            then begin
              if rest < 0
              then Cstruct.blit decoder.buffer (off + rest) decoder.buffer (off + rest - 1) (off - (off + rest));

              off - 1, `Line (expect - 1)
            end else off, `Line expect
          in

          decoder.max <- off;
          decoder.eop <- Some (off + rest);
          p_safe (k ~pkt) decoder
        end else begin
          if off >= Cstruct.len decoder.buffer
          then raise (Invalid_argument "PKT Format: payload upper than 65520 bytes")
          else Read { buffer = decoder.buffer
                    ; off = off
                    ; len = Cstruct.len decoder.buffer - off
                    ; continue = fun n -> loop (rest - n) (off + n) }
        end
      in

      loop (expect - (decoder.max - decoder.pos)) decoder.max
    end

  let p_pkt_len_safe ?(strict = false) k decoder =
    let a = p_hexdigit decoder in
    let b = p_hexdigit decoder in
    let c = p_hexdigit decoder in
    let d = p_hexdigit decoder in

    let expect = (a * (16 * 16 * 16)) + (b * (16 * 16)) + (c * 16) + d in

    if expect = 0
    then begin
      decoder.eop <- Some decoder.pos;
      k ~pkt:`Flush decoder
    end else
      p_pkt_payload ~strict k decoder (expect - 4)

  let p_pkt_line ?(strict = false) k decoder =
    decoder.eop <- None;

    if decoder.max - decoder.pos >= 4
    then p_pkt_len_safe ~strict k decoder
    else begin
      (* compress *)
      if decoder.pos > 0
      then begin
        Cstruct.blit decoder.buffer decoder.pos decoder.buffer 0 (decoder.max - decoder.pos);
        decoder.max <- decoder.max - decoder.pos;
        decoder.pos <- 0;
      end;

      let rec loop off =
        if off - decoder.pos >= 4
        then begin
          decoder.max <- off;
          p_safe (p_pkt_len_safe ~strict k) decoder
        end else begin
          if off >= Cstruct.len decoder.buffer
          then raise (Invalid_argument "PKT Format: payload upper than 65520 bytes")
          else Read { buffer = decoder.buffer
                    ; off = off
                    ; len = Cstruct.len decoder.buffer - off
                    ; continue = fun n -> loop (off + n) }
        end
      in

      loop decoder.max
    end

  let zero_id = String.make Hash.Digest.length '\000' |> Hash.of_string

  let p_hash decoder =
    p_while1 (function '0' .. '9' | 'a' .. 'f' -> true | _ -> false) decoder
    |> Cstruct.to_string
    |> Hash.of_hex

  let not_null = (<>) '\000'

  let p_capability decoder =
    let capability =
      p_while1
        (function '\x61' .. '\x7a' | '0' .. '9' | '-' | '_' -> true | _ -> false)
        decoder
      |> Cstruct.to_string
    in match p_peek_char decoder with
    | Some '=' ->
      p_junk_char decoder;
      let value =
        p_while1 (function '\033' .. '\126' -> true | _ -> false) decoder
        |> Cstruct.to_string in
      Capability.of_string ~value capability
    | _ ->
      Capability.of_string capability

  let p_capabilities1 decoder =
    let acc = [ p_capability decoder ] in

    let rec loop acc = match p_peek_char decoder with
      | Some ' ' ->
        p_junk_char decoder;
        let capability = p_capability decoder in
        loop (capability :: acc)
      | Some chr -> raise (Leave (err_unexpected_char chr decoder))
      | None -> List.rev acc
    in

    loop acc

  let p_reference decoder =
    let refname =
      p_while1
        (function ' ' | '~' | '^' | ':' | '?' | '*' -> false
                  | chr ->
                     let code = Char.code chr in
                     if code  < 31 || code > 126
                     then false
                     else true)
        decoder in
    Reference.of_string (Cstruct.to_string refname)

  let p_first_ref decoder =
    let obj_id = p_hash decoder in
    p_space decoder;
    let reference = p_reference decoder in
    let peeled = match p_peek_char decoder with
      | Some '^' ->
        p_char '^' decoder;
        p_char '{' decoder;
        p_char '}' decoder;
        true
      | Some _ -> false
      | None -> raise (Leave (err_unexpected_end_of_input decoder)) in
    p_null decoder;
    let capabilities =match p_peek_char decoder with
      | Some ' ' ->
        p_junk_char decoder;
        p_capabilities1 decoder
      | Some _ ->
        p_capabilities1 decoder
      | None -> raise (Leave (err_unexpected_end_of_input decoder))
    in

    if Hash.equal obj_id zero_id
    && Reference.to_string reference = "capabilities"
    && peeled
    then `No_ref capabilities
    else `Ref ((obj_id, reference, peeled), capabilities)

  let p_shallow decoder =
    let _ = p_string "shallow" decoder in
    p_space decoder;
    let obj_id = p_hash decoder in
    obj_id

  let p_unshallow decoder =
    let _ = p_string "unshallow" decoder in
    p_space decoder;
    let obj_id = p_hash decoder in
    obj_id

  let p_other_ref decoder =
    let obj_id = p_hash decoder in
    p_space decoder;
    let reference = p_reference decoder in

    let peeled = match p_peek_char decoder with
      | Some '^' ->
        p_char '^' decoder;
        p_char '{' decoder;
        p_char '}' decoder;
        true
      | Some chr -> raise (Leave (err_unexpected_char chr decoder))
      | None -> false in

    (obj_id, reference, peeled)

  type no_line =
    [ `Empty | `Malformed | `Flush ]

  let err_expected_line decoder = function
    | `Empty -> raise (Leave (err_unexpected_empty_pkt_line decoder))
    | `Malformed -> raise (Leave (err_malformed_pkt_line decoder))
    | `Flush -> raise (Leave (err_unexpected_flush_pkt_line decoder))

  type no_line_and_flush =
    [ `Empty | `Malformed ]

  let err_expected_line_or_flush decoder = function
    | #no_line as v -> err_expected_line decoder v

  let p_pkt_flush k decoder =
    p_pkt_line (fun ~pkt decoder -> match pkt with
                                    | #no_line_and_flush as v -> err_expected_line_or_flush decoder v
                                    | `Flush -> k decoder
                                    | `Line _ -> raise (Leave (err_unexpected_pkt_line decoder)))
               decoder

  let p_advertised_refs ~pkt (advertised_refs:Common.advertised_refs) decoder =
    let rec go_shallows ~pkt (advertised_refs:Common.advertised_refs) decoder = match pkt with
      | #no_line_and_flush as v -> err_expected_line_or_flush decoder v
      | `Flush -> p_return advertised_refs decoder
      | `Line _ ->
        match p_peek_char decoder with
        | Some 's' ->
          let hash = p_shallow decoder in
          p_pkt_line (go_shallows { advertised_refs with shallow = hash :: advertised_refs.shallow }) decoder
        | Some chr -> raise (Leave (err_unexpected_char chr decoder))
        | None -> raise (Leave (err_unexpected_end_of_input decoder)) in

    let rec go_other_refs ~pkt (advertised_refs:Common.advertised_refs) decoder = match pkt with
      | #no_line_and_flush as v -> err_expected_line_or_flush decoder v
      | `Flush -> p_return advertised_refs decoder
      | `Line _ ->
        match p_peek_char decoder with
        | Some 's' ->
          go_shallows ~pkt advertised_refs decoder
        | Some _ ->
          let x = p_other_ref decoder in
          p_pkt_line (go_other_refs { advertised_refs with refs = x :: advertised_refs.refs }) decoder
        | None -> raise (Leave (err_unexpected_end_of_input decoder)) in

    let go_first_ref ~pkt (advertised_refs:Common.advertised_refs) decoder = match pkt with
      | #no_line as v -> err_expected_line decoder v
      | `Line _ ->
        match p_first_ref decoder with
        | `No_ref capabilities ->
          p_pkt_line (go_shallows { advertised_refs with capabilities }) decoder
        | `Ref (first, capabilities) ->
          p_pkt_line (go_other_refs { advertised_refs with capabilities
                                                         ; refs = [ first ] }) decoder in

    go_first_ref ~pkt advertised_refs decoder

  let p_http_advertised_refs ~service ~pkt decoder =
    match pkt with
    | #no_line as v -> err_expected_line decoder v
    | `Line _ ->
      ignore @@ p_string "# service=" decoder;
      ignore @@ p_string service decoder;
      p_pkt_line (fun ~pkt decoder -> match pkt with
          | #no_line_and_flush as v -> err_expected_line_or_flush decoder v
          | `Flush ->
            p_pkt_line
              (p_advertised_refs
                 { shallow = []
                 ; refs = []
                 ; capabilities = [] })
              decoder
          | `Line _ as pkt ->
            p_advertised_refs ~pkt
              { shallow = []
              ; refs = []
              ; capabilities = [] }
              decoder)
        decoder

  let p_advertised_refs decoder =
    p_pkt_line
      (p_advertised_refs
         { shallow = []
         ; refs = []
         ; capabilities = [] })
      decoder

  let p_http_advertised_refs ~service decoder =
    p_pkt_line
      (p_http_advertised_refs ~service)
      decoder

  let rec p_shallow_update ~pkt (shallow_update:Common.shallow_update) decoder = match pkt with
    | #no_line_and_flush as v -> err_expected_line_or_flush decoder v
    | `Flush -> p_return shallow_update decoder
    | `Line _ -> match p_peek_char decoder with
      | Some 's' ->
        let x = p_shallow decoder in
        p_pkt_line (p_shallow_update { shallow_update with Common.shallow = x :: shallow_update.shallow }) decoder
      | Some 'u' ->
        let x = p_unshallow decoder in
        p_pkt_line (p_shallow_update { shallow_update with Common.unshallow = x :: shallow_update.unshallow }) decoder
      | Some chr -> raise (Leave (err_unexpected_char chr decoder))
      | None -> raise (Leave (err_unexpected_end_of_input decoder))

  let p_shallow_update decoder =
    p_pkt_line (p_shallow_update { shallow = []
                                 ; unshallow = [] })
      decoder

  let p_multi_ack_detailed decoder =
    ignore @@ p_string "ACK" decoder;
    p_space decoder;
    let hash = p_hash decoder in

    let detail = match p_peek_char decoder with
      | None -> raise (Leave (err_unexpected_end_of_input decoder))
      | Some ' ' ->
        p_junk_char decoder;
        (match p_peek_char decoder with
         | Some 'r' ->
           ignore @@ p_string "ready" decoder;
           `Ready
         | Some 'c' ->
           ignore @@ p_string "common" decoder;
           `Common
         | Some chr -> raise (Leave (err_unexpected_char chr decoder))
         | None -> raise (Leave (err_unexpected_end_of_input decoder)))
      | Some chr -> raise (Leave (err_unexpected_char chr decoder))
    in

    hash, detail

  let p_multi_ack decoder =
    ignore @@ p_string "ACK" decoder;
    p_space decoder;
    let hash = p_hash decoder in
    p_space decoder;
    ignore @@ p_string "continue" decoder;

    hash

  let p_ack decoder =
    ignore @@ p_string "ACK" decoder;
    p_space decoder;
    let hash = p_hash decoder in

    hash

  let p_negociation_result ~pkt k decoder = match pkt with
    | #no_line as v -> err_expected_line decoder v
    | `Line _ ->
      match p_peek_char decoder with
      | Some 'N' ->
        ignore @@ p_string "NAK" decoder;
        k Common.NAK decoder
      | Some 'A' ->
        ignore @@ p_string "ACK" decoder;
        p_space decoder;
        let hash = p_hash decoder in
        k (Common.ACK hash) decoder
      | Some 'E' ->
        ignore @@ p_string "ERR" decoder;
        p_space decoder;
        let msg = Cstruct.to_string @@ p_while1 (fun _ -> true) decoder in
        k (Common.ERR msg) decoder
      | Some chr -> raise (Leave (err_unexpected_char chr decoder))
      | None -> raise (Leave (err_unexpected_end_of_input decoder))

  let p_negociation_one ~pkt ~mode k decoder =
    match pkt with
    | #no_line as v -> err_expected_line decoder v
    | `Line _ ->
      match p_peek_char decoder, mode with
      | Some 's', _ ->
        let x = p_shallow decoder in
        k (`Shallow x) decoder
      | Some 'u', _ ->
        let x = p_unshallow decoder in
        k (`Unshallow x) decoder
      | Some 'A', `Multi_ack_detailed ->
        let (hash, detail) = p_multi_ack_detailed decoder in
        k (`Ack (hash, detail)) decoder
      | Some 'A', `Multi_ack ->
        let hash = p_multi_ack decoder in
        k (`Ack (hash, `Continue)) decoder
      | Some 'A', `Ack ->
        let hash = p_ack decoder in
        k (`Ack (hash, `ACK)) decoder
      | Some 'N', _ ->
        ignore @@ p_string "NAK" decoder;
        k `Nak decoder
      | Some chr, _ -> raise (Leave (err_unexpected_char chr decoder))
      | None, _ -> raise (Leave (err_unexpected_end_of_input decoder))

  let p_negociation ~mode k hashes (acks:Common.acks) decoder =
    let rec go hashes (acks:Common.acks) v decoder = match v with
      | `Shallow hash ->
        let acks = { acks with shallow = hash :: acks.shallow } in
        p_pkt_line (p_negociation_one ~mode (go hashes acks)) decoder
      | `Unshallow hash ->
        let acks = { acks with unshallow = hash :: acks.unshallow } in
        p_pkt_line (p_negociation_one ~mode (go hashes acks)) decoder
      | `Ack (hash, detail) ->
        let hashes = Hash.Set.remove hash hashes in
        let acks = { acks with acks = (hash, detail) :: acks.acks } in

        if Hash.Set.is_empty hashes
        then k { acks with acks = List.rev acks.acks } decoder
        else p_pkt_line (p_negociation_one ~mode (go hashes acks)) decoder
      | `Nak -> k acks decoder in
    p_pkt_line (p_negociation_one ~mode (go hashes acks)) decoder

  let p_negociation ~mode hashes decoder =
    p_negociation ~mode p_return hashes
      { shallow = []
      ; unshallow = []
      ; acks = [] }
      decoder

  let p_pack ~pkt ~mode decoder = match pkt, mode with
    | #no_line_and_flush as v, _ -> err_expected_line_or_flush decoder v
    | `Line n, `No_multiplexe ->
      let raw = Cstruct.sub decoder.buffer decoder.pos n in
      decoder.pos <- decoder.pos + n;
      p_return (`Raw raw) decoder
    | `Flush, _ -> p_return `End decoder
    | `Line n, (`Side_band_64k | `Side_band) ->
      let raw = Cstruct.sub decoder.buffer (decoder.pos + 1) (n - 1) in

      match p_peek_char decoder with
      | Some '\001' -> decoder.pos <- decoder.pos + n; p_return (`Raw raw) decoder
      | Some '\002' -> decoder.pos <- decoder.pos + n; p_return (`Out raw) decoder
      | Some '\003' -> decoder.pos <- decoder.pos + n; p_return (`Err raw) decoder
      | Some chr -> raise (Leave (err_unexpected_char chr decoder))
      | None -> raise (Leave (err_unexpected_end_of_input decoder))

  let p_negociation_result decoder =
    p_pkt_line (p_negociation_result p_return) decoder

  let p_pack ~mode decoder =
    p_pkt_line ~strict:true (p_pack ~mode) decoder

  let p_unpack decoder : (unit, string) result =
    ignore @@ p_string "unpack" decoder;
    p_space decoder;
    let msg = p_while1 (fun _ -> true) decoder in

    match Cstruct.to_string msg with
    | "ok" -> Ok ()
    | err  -> Error err

  let p_command_status decoder : (Reference.t, Reference.t * string) result =
    let status = p_while1 (function ' ' -> false | _ -> true) decoder in

    match Cstruct.to_string status with
    | "ok" ->
      p_space decoder;
      let reference = p_reference decoder in
      Ok reference
    | "ng" ->
      p_space decoder;
      let reference = p_reference decoder in
      p_space decoder;
      let msg = p_while1 (fun _ -> true) decoder |> Cstruct.to_string in
      Error (reference, msg)
    | _ -> raise (Leave (err_unexpected_char '\000' decoder))

  let rec p_http_report_status ~pkt ?unpack ?(commands = []) ~sideband ~references decoder =
    let go_unpack ~pkt k decoder =
      match pkt with
      | #no_line as v -> err_expected_line decoder v
      | `Line _ ->
        match p_peek_char decoder with
        | Some 'u' ->
          let unpack = p_unpack decoder in
          k unpack decoder
        | Some chr -> raise (Leave (err_unexpected_char chr decoder))
        | None -> raise (Leave (err_unexpected_end_of_input decoder)) in

    let go_command ~pkt kcons kfinal decoder =
      match pkt with
      | #no_line_and_flush as v -> err_expected_line_or_flush decoder v
      | `Flush -> kfinal decoder
      | `Line _ ->
        match p_peek_char decoder with
        | Some ('o' | 'n') ->
          let command = p_command_status decoder in
          kcons command decoder
        | Some chr -> raise (Leave (err_unexpected_char chr decoder))
        | None -> raise (Leave (err_unexpected_end_of_input decoder)) in

    match pkt, sideband, unpack with
    | #no_line as v, _, _ -> err_expected_line decoder v
    | `Line _, (`Side_band | `Side_band_64k), unpack ->
      (match p_peek_char decoder with
       | Some '\001' ->
         p_junk_char decoder;
         (match unpack with
          | None ->
            let k unpack decoder =
              p_pkt_line ~strict:true
                (p_http_report_status ~unpack ~commands:[] ~sideband ~references)
                decoder in
            p_pkt_line (go_unpack k) decoder
          | Some unpack ->
            let kcons command decoder =
              p_pkt_line ~strict:true
                (p_http_report_status ~unpack ~sideband ~commands:(command :: commands) ~references)
                decoder in
            let kfinal decoder = p_return { Common.unpack; commands; } decoder in
            p_pkt_line (go_command kcons kfinal) decoder)
       | Some '\002' ->
         ignore @@ p_while0 (fun _ -> true) decoder;
         p_pkt_line (p_http_report_status ?unpack ~commands ~sideband ~references) decoder
       | Some '\003' ->
         ignore @@ p_while0 (fun _ -> true) decoder;
         p_pkt_line (p_http_report_status ?unpack ~commands ~sideband ~references) decoder
       | Some chr ->
         raise (Leave (err_unexpected_char chr decoder))
       | None ->
         raise (Leave (err_unexpected_end_of_input decoder)))
    | `Line _ as pkt, _, None ->
      let k unpack decoder =
        p_pkt_line ~strict:true (p_http_report_status ~unpack ~sideband ~commands:[] ~references) decoder in
      go_unpack ~pkt k decoder
    | `Line _ as pkt, _, Some unpack ->
      let kcons command decoder =
        p_pkt_line ~strict:true (p_http_report_status ~unpack ~sideband ~commands:(command :: commands) ~references) decoder in
      let kfinal decoder = p_return { Common.unpack; commands; } decoder in
      go_command ~pkt kcons kfinal decoder

  let p_http_report_status references sideband decoder =
    p_pkt_line ~strict:true (p_http_report_status ~references ?unpack:None ~commands:[] ~sideband) decoder

  let rec p_report_status ~pkt ~unpack ~commands ~sideband decoder =
    let go unpack commands sideband decoder =
      match p_peek_char decoder with
      | Some 'u' ->
        let unpack = p_unpack decoder in
        p_pkt_line (p_report_status ~unpack:(Some unpack) ~commands ~sideband) decoder
      | Some ('o' | 'n') ->
        let command = p_command_status decoder in
        let commands = match commands with
          | Some lst -> Some (command :: lst)
          | None -> Some [ command ]
        in

        p_pkt_line (p_report_status ~unpack ~commands ~sideband) decoder
      | Some chr -> raise (Leave (err_unexpected_char chr decoder))
      | None -> raise (Leave (err_unexpected_end_of_input decoder))
    in

    match pkt, sideband, unpack, commands with
    | #no_line_and_flush as v, _, _, _  -> err_expected_line_or_flush decoder v
    | `Flush, _, Some unpack, Some (_ :: _ as commands) -> p_return { Common.unpack; commands; } decoder
    | `Flush, _, _, _ -> raise (Leave (err_unexpected_flush_pkt_line decoder))
    | `Line _, (`Side_band | `Side_band_64k), _, _ ->
      (match p_peek_char decoder with
       | Some '\001' ->
         p_junk_char decoder;
         go unpack commands sideband decoder
       | Some '\002' ->
         ignore @@ p_while0 (fun _ -> true) decoder;
         p_pkt_line (p_report_status ~unpack ~commands ~sideband) decoder
       | Some '\003' ->
         ignore @@ p_while0 (fun _ -> true) decoder;
         p_pkt_line (p_report_status ~unpack ~commands ~sideband) decoder
       | Some chr -> raise (Leave (err_unexpected_char chr decoder))
       | None -> raise (Leave (err_unexpected_empty_pkt_line decoder)))
    | `Line _, `No_multiplexe, _, _ ->
      go unpack commands sideband decoder

  let p_report_status sideband decoder =
    p_pkt_line (p_report_status ~unpack:None ~commands:None ~sideband) decoder

  (* XXX(dinosaure): désolé mais ce GADT, c'est quand même la classe. *)
  type _ transaction =
    | HttpReferenceDiscovery : string -> Common.advertised_refs transaction
    | ReferenceDiscovery     : Common.advertised_refs transaction
    | ShallowUpdate          : Common.shallow_update transaction
    | Negociation            : Hash.Set.t * ack_mode -> Common.acks transaction
    | NegociationResult      : Common.negociation_result transaction
    | PACK                   : side_band -> flow transaction
    | ReportStatus           : side_band -> Common.report_status transaction
    | HttpReportStatus       : string list * side_band -> Common.report_status transaction
  and ack_mode =
    [ `Ack | `Multi_ack | `Multi_ack_detailed ]
  and flow =
    [ `Raw of Cstruct.t | `End | `Err of Cstruct.t | `Out of Cstruct.t ]
  and side_band =
    [ `Side_band | `Side_band_64k | `No_multiplexe ]

  let decode
    : type result. decoder -> result transaction -> result state
    = fun decoder -> function
      | HttpReferenceDiscovery service    -> p_safe (p_http_advertised_refs ~service) decoder
      | ReferenceDiscovery                -> p_safe p_advertised_refs decoder
      | ShallowUpdate                     -> p_safe p_shallow_update decoder
      | Negociation (hashes, ackmode)     -> p_safe (p_negociation ~mode:ackmode hashes) decoder
      | NegociationResult                 -> p_safe p_negociation_result decoder
      | PACK sideband                     -> p_safe (p_pack ~mode:sideband) decoder
      | ReportStatus sideband             -> p_safe (p_report_status sideband) decoder
      | HttpReportStatus (refs, sideband) -> p_safe (p_http_report_status refs sideband) decoder

  let decoder () =
    { buffer = Cstruct.create 65535
    ; pos    = 0
    ; max    = 0
    ; eop    = None }
end

module Encoder
    (Hash: S.HASH)
    (Reference: Reference.S)
    (Common: COMMON with type hash := Hash.t and type reference := Reference.t)
  : ENCODER
    with module Hash = Hash
     and module Reference = Reference
     and module Common = Common =
struct
  module Hash = Hash
  module Reference = Reference
  module Common = Common

  type encoder =
    { mutable payload : Cstruct.t
    ; mutable pos     : int }

  let set_pos encoder pos =
    encoder.pos <- pos

  let free { payload; pos; } =
    Cstruct.sub payload pos (Cstruct.len payload - pos)

  type 'a state =
    | Write of { buffer   : Cstruct.t
               ; off      : int
               ; len      : int
               ; continue : int -> 'a state }
    | Ok of 'a

  let flush k encoder =
    if encoder.pos > 0
    then let rec k1 n =
           if n < encoder.pos
           then Write { buffer = encoder.payload
                      ; off = n
                      ; len = encoder.pos - n
                      ; continue = fun m -> k1 (n + m) }
           else begin
             encoder.pos <- 4;
             k encoder
           end
      in
      k1 0
    else
      k encoder

  let writes s k encoder =
    let _len = Cstruct.len encoder.payload in
    let go j l encoder =
      let rem = _len - encoder.pos in
      let len = if l > rem then rem else l in
      Cstruct.blit_from_string s j encoder.payload encoder.pos len;
      encoder.pos <- encoder.pos + len;
      if len < l
      then raise (Invalid_argument "PKT Format: payload upper than 65520 bytes")
      else k encoder
    in
    go 0 (String.length s) encoder

  let w_lf k e = writes "\n" k e

  let noop k encoder = k encoder

  let pkt_line ?(lf = false) writes k encoder =
    let pkt_len encoder =
      let has = encoder.pos in
      let hdr = Fmt.strf "%04x" has in

      Cstruct.blit_from_string hdr 0 encoder.payload 0 4;
      flush k encoder
    in

    writes ((if lf then w_lf else noop) @@ pkt_len) encoder

  let pkt_flush k encoder =
    Cstruct.blit_from_string "0000" 0 encoder.payload 0 4;
    flush k encoder

  let zero_id = String.make Hash.Digest.length '\000' |> Hash.of_string

  let w_space k encoder =
    writes " " k encoder

  let w_null k encoder =
    writes "\000" k encoder

  let w_capabilities lst k encoder =
    let rec loop lst encoder = match lst with
      | [] -> k encoder
      | [ x ] -> writes (Capability.to_string x) k encoder
      | x :: r ->
        (writes (Capability.to_string x)
         @@ w_space
         @@ loop r)
        encoder
    in

    loop lst encoder

  let w_hash hash k encoder =
    writes (Hash.to_hex hash) k encoder

  let w_first_want obj_id capabilities k encoder =
    (writes "want"
     @@ w_space
     @@ w_hash obj_id
     @@ w_space
     @@ w_capabilities capabilities k)
      encoder

  let w_want obj_id k encoder =
    (writes "want"
     @@ w_space
     @@ w_hash obj_id k)
      encoder

  let w_shallow obj_id k encoder =
    (writes "shallow"
     @@ w_space
     @@ w_hash obj_id k)
      encoder

  let w_deepen depth k encoder =
    (writes "deepen"
     @@ w_space
     @@ writes (Fmt.strf "%d" depth) k)
      encoder

  let w_deepen_since timestamp k encoder =
    (writes "deepen-since"
     @@ w_space
     @@ writes (Fmt.strf "%Ld" timestamp) k)
      encoder

  let w_deepen_not reference k encoder =
    (writes "deepen-not"
     @@ w_space
     @@ writes (Reference.to_string reference) k)
      encoder

  let w_first_want ?lf obj_id capabilities k encoder =
    pkt_line ?lf (w_first_want obj_id capabilities) k encoder
  let w_want ?lf obj_id k encoder =
    pkt_line ?lf (w_want obj_id) k encoder
  let w_shallow ?lf obj_id k encoder =
    pkt_line ?lf (w_shallow obj_id) k encoder
  let w_deepen ?lf depth k encoder =
    pkt_line ?lf (w_deepen depth) k encoder
  let w_deepen_since ?lf timestamp k encoder =
    pkt_line ?lf (w_deepen_since timestamp) k encoder
  let w_deepen_not ?lf reference k encoder =
    pkt_line ?lf (w_deepen_not reference) k encoder
  let w_done_and_lf k encoder =
    pkt_line ~lf:true (writes "done") k encoder

  let w_list w l k encoder =
    let rec aux l encoder = match l with
      | [] -> k encoder
      | x :: r ->
        w x (aux r) encoder
    in
    aux l encoder

  let w_upload_request ?lf (upload_request:Common.upload_request) k encoder =
    let first, rest = upload_request.want in

    (w_first_want ?lf first upload_request.capabilities
     @@ (w_list (w_want ?lf) rest)
     @@ (w_list (w_shallow ?lf) upload_request.shallow)
     @@ (match upload_request.deep with
         | Some (`Depth depth)  -> w_deepen ?lf depth
         | Some (`Timestamp t) -> w_deepen_since ?lf t
         | Some (`Ref reference)  -> w_deepen_not ?lf reference
         | None -> noop)
     @@ pkt_flush k)
      encoder

  let w_has hash k encoder =
    (writes "have"
     @@ w_space
     @@ w_hash hash k)
      encoder

  let w_has ?lf hash k encoder = pkt_line ?lf (w_has hash) k encoder

  let w_http_upload_request at_the_end http_upload_request k encoder =
    (w_upload_request ~lf:true { Common.want  = http_upload_request.Common.want
                               ; capabilities = http_upload_request.Common.capabilities
                               ; shallow      = http_upload_request.Common.shallow
                               ; deep         = http_upload_request.Common.deep }
     @@ (w_list (w_has ~lf:true) http_upload_request.has)
     @@ (if at_the_end = `Done
         then w_done_and_lf k
         else pkt_flush k))
      encoder

  let w_flush k encoder =
    pkt_flush k encoder

  let w_request_command request_command k encoder = match request_command with
    | `Upload_pack    -> writes "git-upload-pack"    k encoder
    | `Receive_pack   -> writes "git-receive-pack"   k encoder
    | `Upload_archive -> writes "git-upload-archive" k encoder

  let w_git_proto_request git_proto_request k encoder =
    let w_host host k encoder = match host with
      | Some (host, Some port) ->
        (writes "host="
         @@ writes host
         @@ writes ":"
         @@ writes (Fmt.strf "%d" port)
         @@ w_null k)
          encoder
      | Some (host, None) ->
        (writes "host="
         @@ writes host
         @@ w_null k)
          encoder
      | None -> noop k encoder
    in

    (w_request_command git_proto_request.Common.request_command
     @@ w_space
     @@ writes git_proto_request.pathname
     @@ w_null
     @@ w_host git_proto_request.host k)
      encoder

  let w_done k encoder = pkt_line (writes "done") k encoder

  let w_has hashes k encoder =
    let rec go l encoder = match l with
      | [] -> w_flush k encoder
      | x :: r ->
        (w_has x @@ go r) encoder
    in
    go (Hash.Set.elements hashes) encoder

  let w_git_proto_request git_proto_request k encoder =
    pkt_line (w_git_proto_request git_proto_request) k encoder

  let w_shallows l k encoder =
    let rec go l encoder = match l with
      | [] -> k encoder
      | x :: r ->
        pkt_line
          (fun k -> writes "shallow"
            @@ w_space
            @@ w_hash x k)
          (go r)
          encoder in
    go l encoder

  let w_command command k encoder =
    match command with
    | Common.Create (hash, reference) ->
      (w_hash zero_id
       @@ w_space
       @@ w_hash hash
       @@ w_space
       @@ writes (Reference.to_string reference) k)
        encoder
    | Common.Delete (hash, reference) ->
      (w_hash hash
       @@ w_space
       @@ w_hash zero_id
       @@ w_space
       @@ writes (Reference.to_string reference) k)
        encoder
    | Common.Update (old_id, new_id, reference) ->
      (w_hash old_id
       @@ w_space
       @@ w_hash new_id
       @@ w_space
       @@ writes (Reference.to_string reference) k)
        encoder

  let w_first_command capabilities first k encoder =
    (w_command first
     @@ w_null
     @@ w_capabilities capabilities k)
      encoder

  let w_first_command capabilities first k encoder =
    pkt_line (w_first_command capabilities first) k encoder

  let w_command command k encoder =
    pkt_line (w_command command) k encoder

  let w_commands capabilities (first, rest) k encoder =
    (w_first_command capabilities first
     @@ w_list w_command rest
     @@ pkt_flush k)
      encoder

  let w_push_certificates capabilities push_cert k encoder =
    (* XXX(dinosaure): clean this code, TODO! *)

    ((fun k e -> pkt_line ~lf:true (fun k -> writes "push-cert" @@ w_null @@ w_capabilities capabilities k) k e)
     @@ (fun k e -> pkt_line ~lf:true (writes "certificate version 0.1") k e)
     @@ (fun k e -> pkt_line ~lf:true (fun k -> writes "pusher" @@ w_space @@ writes push_cert.Common.pusher k) k e)
     @@ (fun k e -> pkt_line ~lf:true (fun k -> writes "pushee" @@ w_space @@ writes push_cert.Common.pushee k) k e)
     @@ (fun k e -> pkt_line ~lf:true (fun k -> writes "nonce" @@ w_space @@ writes push_cert.Common.nonce k) k e)
     @@ (fun k e -> w_list (fun x k e -> pkt_line ~lf:true (fun k -> writes "push-option" @@ w_space @@ writes x k) k e) push_cert.Common.options k e)
     @@ (fun k e -> pkt_line ~lf:true noop k e)
     @@ (fun k e -> w_list (fun x k e -> pkt_line ~lf:true (w_command x) k e) push_cert.Common.commands k e)
     @@ (fun k e -> w_list (fun x k e -> pkt_line ~lf:true (writes x) k e) push_cert.Common.gpg k e)
     @@ (fun k e -> pkt_line ~lf:true (writes "push-cert-end") k e)
     @@ pkt_flush
     @@ k)
    encoder

  let w_update_request (update_request:Common.update_request) k encoder =
    (w_shallows update_request.Common.shallow
     @@ (match update_request.Common.requests with
         | `Raw commands   -> w_commands update_request.Common.capabilities commands
         | `Cert push_cert -> w_push_certificates update_request.Common.capabilities push_cert)
     @@ k)
      encoder

  let flush_pack k encoder =
    if encoder.pos > 0
    then let rec k1 n =
           if n < encoder.pos
           then Write { buffer = encoder.payload
                      ; off = n
                      ; len = encoder.pos - n
                      ; continue = fun m -> k1 (n + m) }
           else begin
             encoder.pos <- 0;
             k encoder
           end
      in
      k1 0
    else
      k encoder

  let w_pack n k encoder =
    encoder.pos <- encoder.pos + n;
    flush_pack k encoder

  let w_http_update_request i k encoder =
    w_update_request i k encoder

  type action =
    [ `GitProtoRequest    of Common.git_proto_request
    | `UploadRequest      of Common.upload_request
    | `HttpUploadRequest  of [ `Done | `Flush ] * Common.http_upload_request
    | `UpdateRequest      of Common.update_request
    | `HttpUpdateRequest  of Common.update_request
    | `Has                of Hash.Set.t
    | `Done
    | `Flush
    | `PACK               of int
    | `Shallow            of Hash.t list ]

  let encode encoder = function
    | `GitProtoRequest c        -> w_git_proto_request c         (fun _ -> Ok ()) encoder
    | `UploadRequest i          -> w_upload_request i            (fun _ -> Ok ()) encoder
    | `HttpUploadRequest (v, i) -> w_http_upload_request v i     (fun _ -> Ok ()) encoder
    | `Advertised_refs v        -> w_advertised_refs v           (fun _ -> Ok ()) encoder
    | `Shallow_update v         -> w_shallow_update v            (fun _ -> Ok ()) encoder
    | `Negociation v            -> w_negociation v               (fun _ -> Ok ()) encoder
    | `Negociation_result v     -> w_negociation_result v        (fun _ -> Ok ()) encoder
    | `Report_status (s, v)     -> w_report_status ~sideband:s v (fun _ -> Ok ()) encoder
    | `UpdateRequest i          -> w_update_request i            (fun _ -> Ok ()) encoder
    | `HttpUpdateRequest i      -> w_http_update_request i       (fun _ -> Ok ()) encoder
    | `Has l                    -> w_has l                       (fun _ -> Ok ()) encoder
    | `Done                     -> w_done                        (fun _ -> Ok ()) encoder
    | `Flush                    -> w_flush                       (fun _ -> Ok ()) encoder
    | `Shallow l                -> w_shallows l                  (fun _ -> Ok ()) encoder
    | `PACK n                   -> w_pack n                      (fun _ -> Ok ()) encoder

  let encoder () =
    { payload = Cstruct.create 65535
    ; pos     = 4 }
end

module Client (Hash: S.HASH) (Reference: Reference.S)
  : CLIENT
    with module Hash = Hash
     and module Reference = Reference =
struct
  module Hash = Hash
  module Reference = Reference
  module Common = Common(Hash)(Reference)
  module Decoder = Decoder(Hash)(Reference)(Common)
  module Encoder = Encoder(Hash)(Reference)(Common)

  type context =
    { decoder      : Decoder.decoder
    ; encoder      : Encoder.encoder
    ; mutable capabilities : Capability.t list }

  let capabilities { capabilities; _ } = capabilities
  let set_capabilities context capabilities =
    context.capabilities <- capabilities

  let encode x k ctx =
    let rec loop = function
      | Encoder.Write { buffer; off; len; continue; } ->
        `Write (buffer, off, len, fun n -> loop (continue n))
      | Encoder.Ok () -> k ctx
    in
    loop (Encoder.encode ctx.encoder x)

  let decode phase k ctx =
    let rec loop = function
      | Decoder.Ok v -> k v ctx
      | Decoder.Read { buffer; off; len; continue; } ->
        `Read (buffer, off, len, fun n -> loop (continue n))
      | Decoder.Error { err; buf; committed; } ->
        `Error (err, buf, committed)
    in
    loop (Decoder.decode ctx.decoder phase)

  type result =
    [ `Refs of Common.advertised_refs
    | `ShallowUpdate of Common.shallow_update
    | `Negociation of Common.acks
    | `NegociationResult of Common.negociation_result
    | `PACK of Decoder.flow
    | `Flush
    | `Nothing
    | `ReadyPACK of Cstruct.t
    | `ReportStatus of Common.report_status ]

  type process =
    [ `Read  of (Cstruct.t * int * int * (int -> process ))
    | `Write of (Cstruct.t * int * int * (int -> process))
    | `Error of (Decoder.error * Cstruct.t * int)
    | result ]

  let pp_result ppf = function
    | `Refs refs ->
      Fmt.pf ppf "(`Refs %a)" (Fmt.hvbox Common.pp_advertised_refs) refs
    | `ShallowUpdate shallow_update ->
      Fmt.pf ppf "(`ShallowUpdate %a)" (Fmt.hvbox Common.pp_shallow_update) shallow_update
    | `Negociation acks ->
      Fmt.pf ppf "(`Negociation %a)"
        (Fmt.hvbox Common.pp_acks) acks
    | `NegociationResult result ->
      Fmt.pf ppf "(`NegociationResult %a)" (Fmt.hvbox Common.pp_negociation_result) result
    | `PACK (`Err _) ->
      Fmt.pf ppf "(`Pack stderr)"
    | `PACK (`Out _) ->
      Fmt.pf ppf "(`Pack stdout)"
    | `PACK (`Raw _) ->
      Fmt.pf ppf "(`Pack pack)"
    | `PACK `End ->
      Fmt.pf ppf "(`Pack `End)"
    | `Flush ->
      Fmt.pf ppf "`Flush"
    | `Nothing ->
      Fmt.pf ppf "`Nothing"
    | `ReadyPACK _ ->
      Fmt.pf ppf "(`ReadyPACK #raw)"
    | `ReportStatus status ->
      Fmt.pf ppf "(`ReportStatus %a)" (Fmt.hvbox Common.pp_report_status) status

  type action =
    [ `GitProtoRequest of Common.git_proto_request
    | `Shallow of Hash.t list
    | `UploadRequest of Common.upload_request
    | `UpdateRequest of Common.update_request
    | `Has of Hash.Set.t
    | `Done
    | `Flush
    | `ReceivePACK
    | `SendPACK of int
    | `FinishPACK ]

  let run context = function
    | `GitProtoRequest c ->
      encode
        (`GitProtoRequest c)
        (decode Decoder.ReferenceDiscovery
           (fun refs ctx ->
              ctx.capabilities <- refs.Common.capabilities;
              `Refs refs))
        context
    | `Flush ->
      encode `Flush (fun _ -> `Flush) context
    | `UploadRequest (descr : Common.upload_request) ->
      let common = List.filter (fun x -> List.exists ((=) x) context.capabilities) descr.Common.capabilities in
      (* XXX(dinosaure): we update with the shared capabilities between the
         client and the server. *)

      context.capabilities <- common;

      let next = match descr.Common.deep with
        | Some (`Depth n) ->
          if n > 0
          then decode Decoder.ShallowUpdate (fun shallow_update _ -> `ShallowUpdate shallow_update)
          else (fun _ -> `ShallowUpdate { Common.shallow = []; unshallow = []; })
        | _ -> (fun _ -> `ShallowUpdate { Common.shallow = []; unshallow = []; })
      in
      encode (`UploadRequest descr) next context
    | `UpdateRequest (descr:Common.update_request) ->
      let common = List.filter (fun x -> List.exists ((=) x) context.capabilities) descr.Common.capabilities in

      (* XXX(dinosaure): same as below. *)

      context.capabilities <- common;

      encode (`UpdateRequest descr) (fun { encoder; _ } ->
          Encoder.set_pos encoder 0;
          let raw = Encoder.free encoder in

          `ReadyPACK raw) context
    | `Has has ->
      let ackmode =
        if List.exists ((=) `Multi_ack_detailed) context.capabilities
        then `Multi_ack_detailed
        else if List.exists ((=) `Multi_ack) context.capabilities
        then `Multi_ack
        else `Ack
      in

      encode (`Has has) (decode (Decoder.Negociation (has, ackmode)) (fun status _ -> `Negociation status)) context
    | `Done ->
      encode `Done (decode Decoder.NegociationResult (fun result _ -> `NegociationResult result)) context
    | `ReceivePACK ->
      let sideband =
        if List.exists ((=) `Side_band_64k) context.capabilities
        then `Side_band_64k
        else if List.exists ((=) `Side_band) context.capabilities
        then `Side_band
        else `No_multiplexe
      in
      (decode (Decoder.PACK sideband) (fun flow _ -> `PACK flow)) context
    | `SendPACK w ->
      encode (`PACK w)
        (fun { encoder; _ } ->
          Encoder.set_pos encoder 0;
          let raw = Encoder.free encoder in

          `ReadyPACK raw)
        context
    | `FinishPACK ->
      let sideband =
        if List.exists ((=) `Side_band_64k) context.capabilities
        then `Side_band_64k
        else if List.exists ((=) `Side_band) context.capabilities
        then `Side_band
        else `No_multiplexe
      in

      if List.exists ((=) `Report_status) context.capabilities
      then decode (Decoder.ReportStatus sideband) (fun result _ -> `ReportStatus result) context
      else `Nothing
    (* XXX(dinosaure): the specification does not explain what the server send
       when we don't have the capability [report-status]. *)
    | `Shallow l ->
      encode (`Shallow l) (fun _ -> `Nothing) context

  let context c =
    let context =
      { decoder = Decoder.decoder ()
      ; encoder = Encoder.encoder ()
      ; capabilities = [] }
    in

    context, encode (`GitProtoRequest c)
      (decode Decoder.ReferenceDiscovery
         (fun refs ctx ->
            ctx.capabilities <- refs.Common.capabilities;
            `Refs refs))
      context
end
