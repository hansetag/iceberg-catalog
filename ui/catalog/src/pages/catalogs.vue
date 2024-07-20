<template>
  <v-container>
    <v-row>
      <v-col cols="12">
        <v-treeview
          :items="treeItems.items"
          item-key="id"
          item-title="title"
          expand-on-click
        >
          <template #prepend="{ item }">
            <v-icon v-if="item.children && item.children.length"
              >mdi-bookmark</v-icon
            >
            <v-icon v-else>mdi-database</v-icon>
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

interface TreeItem {
  id: string;
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
        const warehousesResponse = (await loadData(
          managementUrl + "/warehouse?project-id=" + project.project_id
        )) as { warehouses: Warehouse[] };

        const children: TreeItem[] = warehousesResponse.warehouses.map(
          (warehouse) => ({
            id: warehouse.id,
            title: warehouse.name,
          })
        );

        transformedData.push({
          id: project.project_id,
          title: `Project ${project.project_id}`,
          children: children,
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
): Promise<Data | { warehouses: Warehouse[] }> {
  const res = await fetch(subPath);
  if (!res.ok) {
    throw new Error(`HTTP error! status: ${res.status}`);
  }
  return await res.json();
}
</script>
