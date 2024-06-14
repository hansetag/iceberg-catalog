# Customizing

As Customizability is one of the core features we are missing in other Iceberg Catalog implementations, we try to do things differently. The core implementation of this crate is based on four modules that back the `axum` service router:
* `Catalog` is the main Database Backend where Warehouses, Namespaces, Tables and other entities are managed
* `SecretStore` stores credentials that might be required to access the storage of the warehouse
* `AuthZHandler` is used to determine if a certain principal is authorized for an operation
* `EventPublisher` emits events to Message Queues so that external systems can react on changes to your tables
* `ContractValidator` allows an external system to prohibit changes to tables if, for example, data contracts are violated

All components come pre-implemented, however we encourage you to write custom implementations, for example to seamlessly grant access to tables via your companies Data Governance solution, or publish events to your very important messaging service.
