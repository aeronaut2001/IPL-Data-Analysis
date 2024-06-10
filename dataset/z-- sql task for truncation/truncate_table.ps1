# Define the connection string
$serverName = "DESKTOP-FE3PQEO\SQLEXPRESS"
$databaseName = "IPL_PROJECT_SQL"
$username = "sa"
$password = "762001"
$connectionString = "Server=$serverName;Database=$databaseName;User Id=$username;Password=$password;"

# Define the SQL query
$query = "TRUNCATE TABLE sysssislog"

# Load the .NET SQL client library
Add-Type -AssemblyName "System.Data"

# Create a new SQL connection object
$connection = New-Object System.Data.SqlClient.SqlConnection
$connection.ConnectionString = $connectionString

try {
    # Open the SQL connection
    $connection.Open()

    # Create a new SQL command object
    $command = $connection.CreateCommand()
    $command.CommandText = $query

    # Execute the SQL command
    $command.ExecuteNonQuery()

    Write-Output "Table truncated successfully."
}
catch {
    Write-Error "An error occurred: $_"
}
finally {
    # Close the SQL connection
    $connection.Close()
}
