<template>
  <v-container>
    <v-row>
      <v-col cols="12">
        <v-treeview
          :items="treeItems.items"
          item-key="id"
          item-title="title"
          :load-children="fetchUsers"
          expand-on-click
        >
          <template #prepend="{ item }">
            <v-icon v-if="item.itemType == 'project'"> mdi-bookmark</v-icon>
            <v-icon v-else-if="item.itemType == 'warehouse'">
              mdi-database</v-icon
            >
            <v-icon v-else-if="item.itemType == 'namespace'">
              mdi-folder</v-icon
            >
            <v-icon v-else>mdi-table</v-icon>
          </template>
        </v-treeview>
      </v-col>
    </v-row>
  </v-container>
</template>

<script lang="ts" setup>
import { ref, onMounted } from "vue";

interface Project {
  project_id: string;
  warehouses?: Warehouse[];
}

interface Data {
  projects: Project[];
}

interface StorageProfile {
  type: string;
  bucket: string;
  "key-prefix": string;
  "assume-role-arn": string | null;
  endpoint: string;
  region: string;
  "path-style-access": boolean;
}

interface Warehouse {
  id: string;
  name: string;
  "project-id": string;
  "storage-profile": StorageProfile;
  status: string;
}

interface Namespaces {
  namespaces: string[][];
}

interface TreeItem {
  id: string;
  itemType: string;
  title: string;
  children?: TreeItem[];
}

interface TreeItems {
  items: TreeItem[];
}

const treeItems = ref<TreeItems>({ items: [] });

const baseUrl = "http://localhost:8080";
const managementUrl = baseUrl + "/management/v1";
const catalogUrl = baseUrl + "/catalog/v1";

onMounted(async () => {
  try {
    const data = (await loadData(managementUrl + "/project")) as Data;
    if (data) {
      const projects = data.projects;
      const transformedData: TreeItem[] = [];

      for (const project of projects) {
        // const warehousesResponse = (await loadData(
        //   managementUrl + "/warehouse?project-id=" + project.project_id
        // )) as { warehouses: Warehouse[] };

        // const children: TreeItem[] = warehousesResponse.warehouses.map(
        //   (warehouse) => ({
        //     id: warehouse.id,
        //     itemType: "warehouse",
        //     title: warehouse.name,
        //   })
        // );

        transformedData.push({
          id: project.project_id,
          itemType: "project",
          title: `Project ${project.project_id}`,
          children: [], //children,
        });
      }

      treeItems.value = { items: transformedData };
    } else {
      console.error("Projects data is not available.");
    }
  } catch (err) {
    console.error("Failed to load data:", err);
  }
});

async function loadData(
  subPath: string
): Promise<Data | { warehouses: Warehouse[] } | Namespaces> {
  const res = await fetch(subPath);
  if (!res.ok) {
    throw new Error(`HTTP error! status: ${res.status}`);
  }
  return await res.json();
}

async function fetchUsers(item: any) {
  console.log(item);

  if (item.itemType == "project") {
    const warehousesResponse = (await loadData(
      managementUrl + "/warehouse?project-id=" + item.id
    )) as { warehouses: Warehouse[] };
    console.log(warehousesResponse);
    const children: TreeItem[] = warehousesResponse.warehouses.map(
      (warehouse) => ({
        id: warehouse.id,
        itemType: "warehouse",
        title: warehouse.name,
        children: [],
      })
    );
    item.children.push(...children);

    return item;
  } else if (item.itemType == "warehouse") {
    const namespacesResponse = (await loadData(
      catalogUrl + "/" + item.id + "/namespaces"
    )) as Namespaces;
    console.log(item, namespacesResponse);
    const children: TreeItem[] = namespacesResponse.namespaces.flatMap(
      (namespaceArray) =>
        namespaceArray.map((namespace) => ({
          id: namespace,
          itemType: "namespace",
          title: namespace,
          children: [],
        }))
    );
    item.children.push(...children);

    return item;
  }
}
</script>
