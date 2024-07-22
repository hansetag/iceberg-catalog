export interface Project {
  project_id: string;
  warehouses?: Warehouse[];
}

export interface Data {
  projects: Project[];
}

export interface StorageProfile {
  type: string;
  bucket: string;
  "key-prefix": string;
  "assume-role-arn": string | null;
  endpoint: string;
  region: string;
  "path-style-access": boolean;
}

export interface Warehouse {
  id: string;
  name: string;
  "project-id": string;
  "storage-profile": StorageProfile;
  status: string;
}

export interface Namespaces {
  namespaces: string[][];
}

export interface Table {
  namespace: string[];
  name: string;
}

export interface Tables {
  identifiers: Table[];
}

export interface TreeItem {
  id: string;
  projectId?: string;
  whId?: string;
  nsId?: string;
  itemType: string;
  title: string;
  children?: TreeItem[];
}

export interface TreeItems {
  items: TreeItem[];
}

//tale and view
export interface MetadataSummary {
  "engine-version": string;
  "engine-name": string;
  "iceberg-version": string;
  "app-id": string;
}

export interface Representation {
  type: string;
  sql: string;
  dialect: string;
}

export interface Version {
  "version-id": number;
  "schema-id": number;
  "timestamp-ms": number;
  summary: MetadataSummary;
  representations: Representation[];
  "default-namespace": any[];
}

export interface VersionLog {
  "version-id": number;
  "timestamp-ms": number;
}

export interface Field {
  id: number;
  name: string;
  required: boolean;
  type: string;
}

export interface Schema {
  "schema-id": number;
  type: string;
  fields: Field[];
}

export interface Metadata {
  "format-version": number;
  "view-uuid": string;
  location: string;
  "current-version-id": number;
  versions: Version[];
  "version-log": VersionLog[];
  schemas: Schema[];
  properties: {
    "spark.query-column-names": string;
    engine_version: string;
    create_engine_version: string;
  };
}

export interface Config {
  "s3.path-style-access": string;
  region: string;
  "s3.remote-signing-enabled": string;
  "client.region": string;
  "s3.region": string;
  "s3.endpoint": string;
}

export interface DataObject {
  "metadata-location": string;
  metadata: Metadata;
  config: Config;
}

//

//table
interface IcebergTableMetadata {
  "metadata-location": string;
  metadata: {
    "format-version": number;
    "table-uuid": string;
    location: string;
    "last-sequence-number": number;
    "last-updated-ms": number;
    "last-column-id": number;
    schemas: IcebergSchema[];
    "current-schema-id": number;
    "partition-specs": IcebergPartitionSpec[];
    "default-spec-id": number;
    "last-partition-id": number;
    properties: { [key: string]: string }; // Use an index signature for dynamic properties
    "current-snapshot-id": number;
    snapshots: IcebergSnapshot[];
    "snapshot-log": IcebergSnapshotLog[];
    "sort-orders": IcebergSortOrder[];
    "default-sort-order-id": number;
    refs: { [key: string]: { "snapshot-id": number; type: string } }; // Use an index signature for dynamic refs
  };
  config: { [key: string]: string }; // Use an index signature for dynamic config options
}

interface IcebergSchema {
  "schema-id": number;
  type: "struct";
  fields: IcebergField[];
}

interface IcebergField {
  id: number;
  name: string;
  required: boolean;
  type: "long" | "string" | "double"; // Use a union type for supported data types
}

interface IcebergPartitionSpec {
  "spec-id": number;
  fields: any[]; // Since fields can be empty, any type is suitable here
}

interface IcebergSnapshot {
  "snapshot-id": number;
  "sequence-number": number;
  "timestamp-ms": number;
  "manifest-list": string;
  summary: {
    operation: string;
    "total-records": string;
    "total-data-files": string;
    "changed-partition-count": string;
    "spark.app.id": string;
    "total-equality-deletes": string;
    "total-files-size": string;
    "total-delete-files": string;
    "total-position-deletes": string;
    "added-records": string;
    "added-data-files": string;
    "added-files-size": string;
  };
  "schema-id": number;
}

interface IcebergSnapshotLog {
  "snapshot-id": number;
  "timestamp-ms": number;
}

interface IcebergSortOrder {
  "order-id": number;
  fields: any[]; // Since fields can be empty, any type is suitable here
}
