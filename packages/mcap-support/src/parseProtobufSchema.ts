// SPDX-FileCopyrightText: Copyright (C) 2023-2025 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)<lichtblick@bmwgroup.com>
// SPDX-License-Identifier: MPL-2.0

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

import {
  FileDescriptorSet,
  Message,
  MessageType,
  ScalarType,
  createRegistryFromDescriptors,
} from "@bufbuild/protobuf";

import { protobufDefinitionsToDatatypes, stripLeadingDot } from "./protobufDefinitionsToDatatypes";
import { MessageDefinitionMap } from "./types";

/**
 * Parse a Protobuf binary schema (FileDescriptorSet) and produce datatypes and a deserializer
 * function.
 */
export function parseProtobufSchema(
  schemaName: string,
  schemaData: Uint8Array,
): {
  datatypes: MessageDefinitionMap;
  deserialize: (buffer: ArrayBufferView) => unknown;
} {
  const descriptorSet = FileDescriptorSet.fromBinary(schemaData);
  const registry = createRegistryFromDescriptors(descriptorSet);
  const normalizedSchemaName = stripLeadingDot(schemaName);
  const rootType = registry.findMessage(normalizedSchemaName);
  if (!rootType) {
    throw new Error(`Protobuf schema does not contain an entry for '${schemaName}'.`);
  }

  const deserialize = (data: ArrayBufferView) => {
    const message = rootType.fromBinary(
      new Uint8Array(data.buffer, data.byteOffset, data.byteLength),
    );
    return messageToPlainObject(rootType, message);
  };

  const datatypes: MessageDefinitionMap = new Map();
  protobufDefinitionsToDatatypes(datatypes, rootType);

  if (!datatypes.has(normalizedSchemaName)) {
    throw new Error(
      `Protobuf schema does not contain an entry for '${schemaName}'. The schema name should be fully-qualified, e.g. '${normalizedSchemaName}'.`,
    );
  }

  return { deserialize, datatypes };
}

function messageToPlainObject(type: MessageType, message: Message): unknown {
  if (type.typeName === "google.protobuf.Timestamp" || type.typeName === "google.protobuf.Duration") {
    const seconds = (message as unknown as { seconds: bigint | number }).seconds;
    const nanos = (message as unknown as { nanos: number }).nanos;
    if (typeof seconds === "bigint") {
      if (seconds > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new Error(
          `Timestamps with seconds greater than 2^53-1 are not supported (found seconds=${seconds}, nanos=${nanos})`,
        );
      }
      return { sec: Number(seconds), nsec: nanos };
    }
    return { sec: seconds, nsec: nanos };
  }

  const result: Record<string, unknown> = {};
  const messageRecord = message as unknown as Record<string, unknown>;
  for (const field of type.fields.list()) {
    const value = messageRecord[field.localName];
    result[field.name] = convertFieldValue(field, value);
  }
  return result;
}

function convertFieldValue(field: FieldInfo, value: unknown): unknown {
  switch (field.kind) {
    case "scalar":
      return convertScalarField(field, value);
    case "enum":
      return convertEnumField(field, value);
    case "message":
      if (field.repeated) {
        const arr = Array.isArray(value) ? value : [];
        return arr.map((item) => messageToPlainObject(field.T, item as Message));
      }
      if (value == undefined) {
        return value;
      }
      return messageToPlainObject(field.T, value as Message);
    case "map":
      return convertMapField(field as MapField, value);
  }
}

function convertScalarField(field: FieldInfo & { kind: "scalar" }, value: unknown): unknown {
  if (field.repeated) {
    const arr = Array.isArray(value) ? value : [];
    return arr.map((item) => convertScalarValue(field.T, item));
  }
  return convertScalarValue(field.T, value);
}

function convertEnumField(field: FieldInfo & { kind: "enum" }, value: unknown): unknown {
  if (field.repeated) {
    return Array.isArray(value) ? value : [];
  }
  return value ?? 0;
}

function convertScalarValue(type: number, value: unknown): unknown {
  if (value == undefined) {
    if (type === ScalarType.BOOL) {
      return false;
    }
    if (type === ScalarType.STRING) {
      return "";
    }
    if (type === ScalarType.BYTES) {
      return new Uint8Array();
    }
    if (
      type === ScalarType.INT64 ||
      type === ScalarType.SINT64 ||
      type === ScalarType.SFIXED64 ||
      type === ScalarType.UINT64 ||
      type === ScalarType.FIXED64
    ) {
      return 0n;
    }
    return 0;
  }
  return value;
}

function convertMapField(field: MapField, value: unknown): unknown {
  if (value == undefined) {
    return [];
  }
  const mapValue = value as Record<string, unknown>;
  const entries: unknown[] = [];
  for (const [key, mapEntryValue] of Object.entries(mapValue)) {
    entries.push({
      key: convertMapKey(field.K, key),
      value: convertMapValue(field.V, mapEntryValue),
    });
  }
  return entries;
}

function convertMapKey(type: number, key: string): unknown {
  switch (type) {
    case ScalarType.STRING:
      return key;
    case ScalarType.BOOL:
      return key === "true";
    case ScalarType.INT32:
    case ScalarType.SINT32:
    case ScalarType.SFIXED32:
    case ScalarType.FIXED32:
    case ScalarType.UINT32:
      return Number(key);
    case ScalarType.INT64:
    case ScalarType.SINT64:
    case ScalarType.SFIXED64:
    case ScalarType.UINT64:
    case ScalarType.FIXED64:
      return BigInt(key);
    default:
      return Number(key);
  }
}

function convertMapValue(field: MapField["V"], value: unknown): unknown {
  switch (field.kind) {
    case "scalar":
      return convertScalarValue(field.T, value);
    case "enum":
      return value;
    case "message":
      return value == undefined ? value : messageToPlainObject(field.T, value as Message);
  }
}

type FieldInfo = ReturnType<MessageType["fields"]["list"]>[number];

type MapField = FieldInfo & {
  kind: "map";
  K: number;
  V:
    | { kind: "scalar"; T: number }
    | { kind: "enum"; T: { values: ReadonlyArray<{ name: string; no: number }> } }
    | { kind: "message"; T: MessageType };
};
