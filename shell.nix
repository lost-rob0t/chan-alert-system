{ pkgs ? import <nixpkgs> {} }:

with pkgs;

mkShell {
  buildInputs = [
    sqlite
    nim
    gcc-arm-embedded
  ];
}
