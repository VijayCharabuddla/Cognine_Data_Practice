[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_customers_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Customer_ID", DestinationColumnName = "Customer_ID"], [SourceColumnName = "Name", DestinationColumnName = "Name"], [SourceColumnName = "Email", DestinationColumnName = "Email"], [SourceColumnName = "Country", DestinationColumnName = "Country"], [SourceColumnName = "Signup_date", DestinationColumnName = "Signup_date"], [SourceColumnName = "Last_login", DestinationColumnName = "Last_login"], [SourceColumnName = "public_status", DestinationColumnName = "public_status"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared bronze_customers = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "efa3bb07-4475-4ad7-9705-502ccae6ecd7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "6f9615ba-f51a-4c46-af28-d03e2dd8e262"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze_customers", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"customer_id", "Customer_ID"}, {"Full_name", "Name"}, {"email", "Email"}, {"country", "Country"}, {"signup_date", "Signup_date"}, {"status", "Status"}, {"last_login", "Last_login"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", null, "Unknown", Replacer.ReplaceValue, {"Name"}),
  #"Lowercased text" = Table.TransformColumns(#"Replaced value", {{"Email", each Text.Lower(_), type nullable text}}),
  #"Capitalized each word" = Table.TransformColumns(#"Lowercased text", {{"Country", each Text.Proper(_), type nullable text}}),
  #"Removed duplicates" = Table.Distinct(#"Capitalized each word", {"Customer_ID"}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Removed duplicates", "public_status", each if [Status]="A" then "Active"
else if [Status]="I" then "Inactive"
else "Unknown"), {{"public_status", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Added custom", {"Status"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Removed columns", null, #date(2026, 1, 22), Replacer.ReplaceValue, {"Signup_date"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", null, #date(2026, 1, 22), Replacer.ReplaceValue, {"Last_login"})
in
  #"Replaced value 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_products_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "product_id", DestinationColumnName = "product_id"], [SourceColumnName = "Customer_ID", DestinationColumnName = "Customer_ID"], [SourceColumnName = "Prod_name", DestinationColumnName = "Prod_name"], [SourceColumnName = "Category", DestinationColumnName = "Category"], [SourceColumnName = "price", DestinationColumnName = "price"], [SourceColumnName = "currency", DestinationColumnName = "currency"], [SourceColumnName = "product_status", DestinationColumnName = "product_status"], [SourceColumnName = "created_date", DestinationColumnName = "created_date"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared bronze_products = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "efa3bb07-4475-4ad7-9705-502ccae6ecd7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "6f9615ba-f51a-4c46-af28-d03e2dd8e262"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze_products", ItemKind = "Table"]}[Data],
  #"Capitalized each word" = Table.TransformColumns(#"Navigation 2", {{"Prod_name", each Text.Proper(_), type nullable text}}),
  #"Capitalized each word 1" = Table.TransformColumns(#"Capitalized each word", {{"Category", each Text.Proper(_), type nullable text}}),
  #"Replaced value" = Table.ReplaceValue(#"Capitalized each word 1", null, #date(2026, 1, 22), Replacer.ReplaceValue, {"created_date"}),
  #"Added custom" = Table.AddColumn(#"Replaced value", "product_status", each if [status]="A" then "Active"
else if [status]="I" then "Inactive"
else "Unknown"),
  #"Removed columns" = Table.RemoveColumns(#"Added custom", {"status"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns", {"product_id", "Prod_name", "Category", "price", "currency", "product_status", "created_date"}),
  #"Added index" = Table.AddIndexColumn(#"Reordered columns", "Index", 101, 1, Int64.Type),
  #"Renamed columns" = Table.RenameColumns(#"Added index", {{"Index", "Customer_ID"}}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Renamed columns", {"product_id", "Customer_ID", "Prod_name", "Category", "price", "currency", "product_status", "created_date"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Reordered columns 1", {{"product_status", type text}})
in
  #"Changed column type";
shared bronze_customers_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "efa3bb07-4475-4ad7-9705-502ccae6ecd7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "6f9615ba-f51a-4c46-af28-d03e2dd8e262"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_customers_transformed", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared bronze_products_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "efa3bb07-4475-4ad7-9705-502ccae6ecd7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "6f9615ba-f51a-4c46-af28-d03e2dd8e262"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_products_transformed", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
