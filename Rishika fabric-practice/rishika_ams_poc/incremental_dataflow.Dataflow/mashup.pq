[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "staging Services_Stg_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "__$operation", DestinationColumnName = "__$operation"], [SourceColumnName = "service_id", DestinationColumnName = "service_id"], [SourceColumnName = "service_name", DestinationColumnName = "service_name"], [SourceColumnName = "service_owner_id", DestinationColumnName = "service_owner_id"], [SourceColumnName = "criticality", DestinationColumnName = "criticality"], [SourceColumnName = "active_flag", DestinationColumnName = "active_flag"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared #"staging Services_Stg" = let
  Source = Sql.Database("COGNINE-L75", "TicketingPOC"),
  #"Navigation 1" = Source{[Schema = "staging", Item = "Services_Stg"]}[Data]
in
  #"Navigation 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "staging SupportGroups_Stg_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "__$operation", DestinationColumnName = "__$operation"], [SourceColumnName = "group_id", DestinationColumnName = "group_id"], [SourceColumnName = "group_name", DestinationColumnName = "group_name"], [SourceColumnName = "location", DestinationColumnName = "location"], [SourceColumnName = "active_flag", DestinationColumnName = "active_flag"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared #"staging SupportGroups_Stg" = let
  Source = Sql.Database("COGNINE-L75", "TicketingPOC"),
  #"Navigation 1" = Source{[Schema = "staging", Item = "SupportGroups_Stg"]}[Data]
in
  #"Navigation 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "staging Tickets_Stg_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "__$operation", DestinationColumnName = "__$operation"], [SourceColumnName = "ticket_id", DestinationColumnName = "ticket_id"], [SourceColumnName = "ticket_type", DestinationColumnName = "ticket_type"], [SourceColumnName = "opened_date", DestinationColumnName = "opened_date"], [SourceColumnName = "closed_date", DestinationColumnName = "closed_date"], [SourceColumnName = "ticket_status", DestinationColumnName = "ticket_status"], [SourceColumnName = "requester_id", DestinationColumnName = "requester_id"], [SourceColumnName = "assigned_group_id", DestinationColumnName = "assigned_group_id"], [SourceColumnName = "service_id", DestinationColumnName = "service_id"], [SourceColumnName = "category_id", DestinationColumnName = "category_id"], [SourceColumnName = "priority", DestinationColumnName = "priority"], [SourceColumnName = "impact", DestinationColumnName = "impact"], [SourceColumnName = "urgency", DestinationColumnName = "urgency"], [SourceColumnName = "sla_id", DestinationColumnName = "sla_id"], [SourceColumnName = "location", DestinationColumnName = "location"], [SourceColumnName = "short_description", DestinationColumnName = "short_description"], [SourceColumnName = "description", DestinationColumnName = "description"], [SourceColumnName = "record_created_at", DestinationColumnName = "record_created_at"], [SourceColumnName = "record_updated_at", DestinationColumnName = "record_updated_at"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared #"staging Tickets_Stg" = let
  Source = Sql.Database("COGNINE-L75", "TicketingPOC"),
  #"Navigation 1" = Source{[Schema = "staging", Item = "Tickets_Stg"]}[Data]
in
  #"Navigation 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "staging TicketWorklogs_Stg_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "__$operation", DestinationColumnName = "__$operation"], [SourceColumnName = "worklog_id", DestinationColumnName = "worklog_id"], [SourceColumnName = "ticket_id", DestinationColumnName = "ticket_id"], [SourceColumnName = "updated_by", DestinationColumnName = "updated_by"], [SourceColumnName = "update_type", DestinationColumnName = "update_type"], [SourceColumnName = "update_timestamp", DestinationColumnName = "update_timestamp"], [SourceColumnName = "worklog_comment", DestinationColumnName = "worklog_comment"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared #"staging TicketWorklogs_Stg" = let
  Source = Sql.Database("COGNINE-L75", "TicketingPOC"),
  #"Navigation 1" = Source{[Schema = "staging", Item = "TicketWorklogs_Stg"]}[Data]
in
  #"Navigation 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "staging Users_Stg_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "__$operation", DestinationColumnName = "__$operation"], [SourceColumnName = "user_id", DestinationColumnName = "user_id"], [SourceColumnName = "user_type", DestinationColumnName = "user_type"], [SourceColumnName = "first_name", DestinationColumnName = "first_name"], [SourceColumnName = "last_name", DestinationColumnName = "last_name"], [SourceColumnName = "email", DestinationColumnName = "email"], [SourceColumnName = "location", DestinationColumnName = "location"], [SourceColumnName = "active_flag", DestinationColumnName = "active_flag"], [SourceColumnName = "is_deleted", DestinationColumnName = "is_deleted"], [SourceColumnName = "created_date", DestinationColumnName = "created_date"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared #"staging Users_Stg" = let
  Source = Sql.Database("COGNINE-L75", "TicketingPOC"),
  #"Navigation 1" = Source{[Schema = "staging", Item = "Users_Stg"]}[Data]
in
  #"Navigation 1";
shared #"staging Services_Stg_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4f3a671f-b3bc-4df2-abed-b625a3e18722"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8ac491e8-04d6-45d2-8319-8db2f709c0e7"]}[Data],
  TableNavigation = Navigation_2{[Id = "delta_Services_Stg", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"staging SupportGroups_Stg_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4f3a671f-b3bc-4df2-abed-b625a3e18722"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8ac491e8-04d6-45d2-8319-8db2f709c0e7"]}[Data],
  TableNavigation = Navigation_2{[Id = "delta_SupportGroups_Stg", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"staging Tickets_Stg_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4f3a671f-b3bc-4df2-abed-b625a3e18722"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8ac491e8-04d6-45d2-8319-8db2f709c0e7"]}[Data],
  TableNavigation = Navigation_2{[Id = "staging_Tickets_Stg", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"staging TicketWorklogs_Stg_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4f3a671f-b3bc-4df2-abed-b625a3e18722"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8ac491e8-04d6-45d2-8319-8db2f709c0e7"]}[Data],
  TableNavigation = Navigation_2{[Id = "delta_TicketWorklogs_Stg", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"staging Users_Stg_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4f3a671f-b3bc-4df2-abed-b625a3e18722"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8ac491e8-04d6-45d2-8319-8db2f709c0e7"]}[Data],
  TableNavigation = Navigation_2{[Id = "delta_Users_Stg", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
