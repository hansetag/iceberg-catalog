<template>
  <div>Catalogs</div>
  {{ data }}
</template>

<script lang="ts" setup>
//

const url = "http://localhost:8080/management/v1/";

const data = ref(null);
const error = ref(null);

onMounted(() => {
  fetch(url + "/project")
    .then((res) => res.json())
    .then((json) => (data.value = json))
    .catch((err) => (error.value = err));
});
</script>
