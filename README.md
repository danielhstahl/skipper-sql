Work in progress.

Allows upload of files from skipper/sails into MSSQL.  This  requires a table with a varbinary(max) column and a primary key (Which may be over any number of fields).  Optionally, other fields may be inserted as well.  Example usage:

```javascript
req.file('avatar').upload({
    adapter:require('skipper-sql'),
    config:sails.config.connections.MySqlServer, //this is the server defined in connections
    parameters:{myField1:value1, myField2:value2}, //optional values to insert into same row as file
    fileField:'myAttachment', //required: name of column which holds the file (type of varbinary(max))
    sqlTable:'myTable', //required: name of table which holds the attachments
    callback:myFunction //optional callback after finished reading the file
});
```