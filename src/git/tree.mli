module type S =
sig
  module Digest
    : Ihash.IDIGEST
  (** The [Digest] module used to make the module. *)

  module Hash
    : Common.BASE
  (** The Hash module *)

  type entry =
    { perm : perm
    ; name : string
    ; node : Hash.t }
  and perm =
    [ `Normal (** A {!Blob.t}. *)
    | `Everybody
    | `Exec   (** An executable. *)
    | `Link   (** A {!Blob.t} that specifies the path of a {i symlink}. *)
    | `Dir    (** A sub-{!Tree.t}. *)
    | `Commit (** A sub-module ({!Commit.t}). *)
    ]
  and t = entry list
  (** A Git Tree object. Git stores content in a manner similar to a UNIX {i
      filesystem}, but a bit simplified. All the content is stored as tree and
      {Blob.t} objects, with trees corresponding to UNIX directory entries and blobs
      corresponding more or less to {i inodes} or file contents. A single tree
      object contains one or more tree entries, each of which contains a hash
      pointer to a {Blob.t} or sub-tree with its associated mode, type, and
      {i filename}. *)

  module D
    : Common.DECODER  with type t = t
                       and type raw = Cstruct.t
                       and type init = Cstruct.t
                       and type error = [ `Decoder of string ]
  (** The decoder of the Git Tree object. We constraint the input to be a
      {Cstruct.t}. This decoder needs a {Cstruct.t} as an internal buffer. *)

  module A
    : Common.ANGSTROM with type t = t
  (** The Angstrom decoder of the Git Tree object. *)

  module F
    : Common.FARADAY  with type t = t
  (** The Faraday encoder of the Git Tree object. *)

  module M
    : Common.MINIENC  with type t = t
  (** The {!Minienc} encoder of the Git Tree object. *)

  module E : Common.ENCODER  with type t = t
                              and type raw = Cstruct.t
                              and type init = int * t
                              and type error = [ `Never ]
  (** The encoder (which uses a {Minienc.encoder}) of the Git Tree object. We
      constraint the output to be a {Cstruct.t}. This encoder needs the Tree OCaml
      value and the memory consumption of the encoder (in bytes). The encoder can
      not fail.

      NOTE: we can not unspecified the error type (it needs to be concrete) but,
      because the encoder can not fail, we define the error as [`Never]. *)

  include Ihash.DIGEST with type t := t and type hash = Hash.t
  include Common.BASE with type t := t

  val hashes : t -> Hash.t list
  (** [hashes t] returns all pointer of the tree [t]. *)
end

module Make
    (Digest : Ihash.IDIGEST with type t = Bytes.t
                             and type buffer = Cstruct.t)
  : S with type Hash.t = Digest.t
       and module Digest = Digest
(** The {i functor} to make the OCaml representation of the Git Tree object by a
    specific hash implementation. We constraint the {!IDIGEST} module to
    generate a {!Bytes.t} and compute a {Cstruct.t}. *)