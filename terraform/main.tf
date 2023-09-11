provider "azurerm" {
  features {}
}

variable "resource_group_name" {
  description = "Resource Group for Postgresql CDC POC"
  default     = "pg-cdc-poc"
}

variable "location" {
  description = "Località di Azure"
  default     = "West Europe"
}

resource "azurerm_resource_group" "pg-cdc-poc" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_postgresql_flexible_server" "pg-cdc-poc" {
  name                = "psqlserver-cdc-poc"
  location            = azurerm_resource_group.pg-cdc-poc.location
  resource_group_name = azurerm_resource_group.pg-cdc-poc.name

  sku_name = "GP_Standard_D2s_v3"

  storage_mb                   = 32768
  backup_retention_days        = 7
  geo_redundant_backup_enabled = false
  administrator_login          = "psqladminun"
  administrator_password       = "H@Sh1CoR3!"


  version = "13"
}

resource "azurerm_postgresql_flexible_server_configuration" "replication" {
  server_id = azurerm_postgresql_flexible_server.pg-cdc-poc.id
  name      = "wal_level"
  value     = "logical"
}

resource "azurerm_postgresql_flexible_server_configuration" "disable_tls" {
  name      = "require_secure_transport"
  server_id = azurerm_postgresql_flexible_server.pg-cdc-poc.id
  value     = "off"
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "all" {
  name             = "allow-all-ips"
  server_id        = azurerm_postgresql_flexible_server.pg-cdc-poc.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "255.255.255.255"
}

resource "azurerm_postgresql_flexible_server_database" "default" {
  name      = "pg-cdc-poc-db"
  server_id = azurerm_postgresql_flexible_server.pg-cdc-poc.id
  collation = "en_US.utf8"
  charset   = "UTF8"
}

resource "azurerm_eventhub_namespace" "eh-pg-cdc-poc" {
  name                = "eh-pg-cdc-poc"
  location            = azurerm_resource_group.pg-cdc-poc.location
  resource_group_name = azurerm_resource_group.pg-cdc-poc.name
  sku                 = "Standard"
  capacity            = 1
}

resource "azurerm_eventhub" "eventhub-pg-cdc-poc" {
  name                = "eventhub-pg-cdc-poc"
  namespace_name      = azurerm_eventhub_namespace.eh-pg-cdc-poc.name
  resource_group_name = azurerm_resource_group.pg-cdc-poc.name
  partition_count     = 2
  message_retention   = 1
}

resource "azurerm_eventhub_authorization_rule" "eventhub-ar-pg-cdc-poc" {
  name                = "eventhub-ar-pg-cdc-poc"
  namespace_name      = azurerm_eventhub_namespace.eh-pg-cdc-poc.name
  eventhub_name       = azurerm_eventhub.eventhub-pg-cdc-poc.name
  resource_group_name = azurerm_resource_group.pg-cdc-poc.name

  listen = true
  send   = true
  manage = true
}

resource "null_resource" "alter_user" {
  depends_on = [azurerm_postgresql_flexible_server_database.default]
  provisioner "local-exec" {
    command = "./alterUser.sh ${azurerm_postgresql_flexible_server.pg-cdc-poc.fqdn} psqladminun ${azurerm_postgresql_flexible_server_database.default.name} H@Sh1CoR3!"
  }
}