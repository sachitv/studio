// SPDX-FileCopyrightText: Copyright (C) 2023-2025 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)<lichtblick@bmwgroup.com>
// SPDX-License-Identifier: MPL-2.0

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
import type { MessageType } from "@bufbuild/protobuf";

type RuntimeField = ReturnType<MessageType["fields"]["list"]>[number];

type MapField = RuntimeField & {
  kind: "map";
  K: number;
  V:
    | { kind: "scalar"; T: number }
    | { kind: "enum"; T: { values: ReadonlyArray<{ name: string; no: number }> } }
    | { kind: "message"; T: MessageType };
};

import { MessageDefinitionField } from "@lichtblick/message-definition";

import { MessageDefinitionMap } from "./types";

function protobufScalarToRosPrimitive(type: string | number): string {
  switch (type) {
    case "double":
    case 1:
      return "float64";
    case "float":
    case 2:
      return "float32";
    case "int32":
    case "sint32":
    case "sfixed32":
    case 5:
    case 17:
    case 15:
      return "int32";
    case "uint32":
    case "fixed32":
    case 13:
    case 7:
      return "uint32";
    case "int64":
    case "sint64":
    case "sfixed64":
    case 3:
    case 18:
    case 16:
      return "int64";
    case "uint64":
    case "fixed64":
    case 4:
    case 6:
      return "uint64";
    case "bool":
    case 8:
      return "bool";
    case "string":
    case 9:
      return "string";
  }
  throw new Error(`Expected protobuf scalar type, got ${type}`);
}

export function stripLeadingDot(typeName: string): string {
  return typeName.replace(/^\./, "");
}

export function protobufDefinitionsToDatatypes(
  datatypes: MessageDefinitionMap,
  type: MessageType,
): void {
  const definitions: MessageDefinitionField[] = [];
  // The empty list reference is added to the map so a `.has` lookup below can prevent infinite recursion on cyclical types
  datatypes.set(stripLeadingDot(type.typeName), { definitions });
  for (const field of type.fields.list()) {
    addFieldDefinition(datatypes, definitions, type, field);
  }
}

function addFieldDefinition(
  datatypes: MessageDefinitionMap,
  definitions: MessageDefinitionField[],
  parent: MessageType,
  field: RuntimeField,
): void {
  if ((field as MapField).kind === "map") {
    addMapFieldDefinition(datatypes, definitions, parent, field as MapField);
    return;
  }
  switch (field.kind) {
    case "enum":
      for (const enumValue of field.T.values) {
        definitions.push({
          name: enumValue.name,
          type: "int32",
          isConstant: true,
          value: enumValue.no,
        });
      }
      definitions.push({ type: "int32", name: field.name });
      break;
    case "message": {
      const messageType = field.T;
      const fullName = stripLeadingDot(messageType.typeName);
      definitions.push({
        type: fullName,
        name: field.name,
        isComplex: true,
        isArray: field.repeated,
      });
      if (!datatypes.has(fullName)) {
        protobufDefinitionsToDatatypes(datatypes, messageType);
      }
      break;
    }
    case "scalar":
      if (parent.typeName === "google.protobuf.Timestamp" || parent.typeName === "google.protobuf.Duration") {
        definitions.push({
          type: "int32",
          name: field.name === "seconds" ? "sec" : "nsec",
          isArray: field.repeated,
        });
        break;
      }
      if (field.T === 12) {
        if (field.repeated) {
          throw new Error("Repeated bytes are not currently supported");
        }
        definitions.push({ type: "uint8", name: field.name, isArray: true });
        break;
      }
      definitions.push({
        type: protobufScalarToRosPrimitive(field.T),
        name: field.name,
        isArray: field.repeated,
      });
      break;
  }
}

function addMapFieldDefinition(
  datatypes: MessageDefinitionMap,
  definitions: MessageDefinitionField[],
  parent: MessageType,
  field: MapField,
): void {
  const entryTypeName = `${stripLeadingDot(parent.typeName)}.${capitalize(field.jsonName)}Entry`;
  definitions.push({
    type: entryTypeName,
    name: field.name,
    isComplex: true,
    isArray: true,
  });

  if (datatypes.has(entryTypeName)) {
    return;
  }

  const entryDefinitions: MessageDefinitionField[] = [];
  entryDefinitions.push({ name: "key", type: protobufScalarToRosPrimitive(field.K) });

  switch (field.V.kind) {
    case "scalar":
      if (field.V.T === 12) {
        entryDefinitions.push({ name: "value", type: "uint8", isArray: true });
      } else {
        entryDefinitions.push({ name: "value", type: protobufScalarToRosPrimitive(field.V.T) });
      }
      break;
    case "enum":
      for (const enumValue of field.V.T.values) {
        entryDefinitions.push({
          name: enumValue.name,
          type: "int32",
          isConstant: true,
          value: enumValue.no,
        });
      }
      entryDefinitions.push({ name: "value", type: "int32" });
      break;
    case "message": {
      const fullName = stripLeadingDot(field.V.T.typeName);
      entryDefinitions.push({ name: "value", type: fullName, isComplex: true });
      if (!datatypes.has(fullName)) {
        protobufDefinitionsToDatatypes(datatypes, field.V.T);
      }
      break;
    }
  }

  datatypes.set(entryTypeName, { definitions: entryDefinitions });
}

function capitalize(value: string): string {
  return value.length === 0 ? value : value[0]!.toUpperCase() + value.slice(1);
}
