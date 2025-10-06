// SPDX-FileCopyrightText: Copyright (C) 2023-2025 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)<lichtblick@bmwgroup.com>
// SPDX-License-Identifier: MPL-2.0

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

function getGlobalAtob(): ((data: string) => string) | undefined {
  return typeof globalThis.atob === "function" ? globalThis.atob : undefined;
}

function getGlobalBtoa(): ((data: string) => string) | undefined {
  return typeof globalThis.btoa === "function" ? globalThis.btoa : undefined;
}

export function decodeBase64(base64: string): Uint8Array {
  const atob = getGlobalAtob();
  if (atob != undefined) {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i += 1) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
  }
  if (typeof Buffer !== "undefined") {
    return Uint8Array.from(Buffer.from(base64, "base64"));
  }
  throw new Error("No base64 decoder available");
}

export function encodeBase64(data: ArrayLike<number>): string {
  const btoa = getGlobalBtoa();
  if (btoa != undefined) {
    let binary = "";
    for (let i = 0; i < data.length; i += 1) {
      binary += String.fromCharCode(data[i]!);
    }
    return btoa(binary);
  }
  if (typeof Buffer !== "undefined") {
    return Buffer.from(data instanceof Uint8Array ? data : Uint8Array.from(data)).toString("base64");
  }
  throw new Error("No base64 encoder available");
}
