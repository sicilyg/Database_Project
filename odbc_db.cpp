#include "odbc_db.h"
#include <string>
using namespace std;

// Connect to the database
void odbc_db::Connect(string Username, string mysqlPassword, string SchemaName)
{
   try
   {
      driver = get_driver_instance();
      connection = driver->connect("tcp://127.0.0.1:3306", Username, mysqlPassword);
      connection->setSchema(SchemaName);
   }
  catch (sql::SQLException &e) 
  {
      cout << "ERROR: SQLException in " << __FILE__;
      cout << " (" << __func__<< ") on line " << __LINE__ << endl;
      cout << "ERROR: " << e.what();
      cout << " (MySQL error code: " << e.getErrorCode();
      cout << ", SQLState: " << e.getSQLState() << ")" << endl;
   }
}

// Disconnect from the database
void odbc_db::disConnect()  
{
   delete resultSet;
   delete statement;
   connection -> close();
   delete connection;
}

// Execute an SQL query passed in as a String parameter
// and print the resulting relation
string odbc_db::query(string q) 
{
string builder = ""; 
   try 
   {
      resultSet = statement->executeQuery(q);
      builder.append("<br>---------------------------------<br>");
      builder.append("Query: <br>" + q + "<br><br>Result: ");
      builder.append(print(resultSet));
   }
   catch (sql::SQLException e) 
   {
      cout << "ERROR: SQLException in " << __FILE__;
      cout << " (" << __func__<< ") on line " << __LINE__ << endl;
      cout << "ERROR: " << e.what();
      cout << " (MySQL error code: " << e.getErrorCode();
      cout << ", SQLState: " << e.getSQLState() << ")" << endl;
   }
   return builder;
}

// Print the results of a query with attribute names on the first line
// Followed by the tuples, one per line
string odbc_db::print (sql::ResultSet *resultSet) 
{
string builder = ""; 
   try
   {
      if (resultSet -> rowsCount() != 0)
      {
         sql::ResultSetMetaData *metaData = resultSet->getMetaData();
         int numColumns = metaData->getColumnCount();
         builder.append(printHeader( metaData, numColumns));
         builder.append(printRecords( resultSet, numColumns));
      }
      else
         throw runtime_error("ResultSetMetaData FAILURE - no records in the result set");
   }
   catch (std::runtime_error &e) 
   {
   }
   return builder;
 }

// Print the attribute names
string odbc_db::printHeader(sql::ResultSetMetaData *metaData, int numColumns)
{ 
string builder = "";

   try 
   {
      //Printing Column names
      for (int i = 1; i <= numColumns; i++) 
      {
         if (i > 1)
            builder.append(",  ");
         builder.append(metaData->getColumnName(i));; //ColumnName
      }
      builder.append("<br>");
   }
   catch (sql::SQLException &e)
   {
   }
   return builder;
}

// Print the attribute values for all tuples in the result
string odbc_db::printRecords(sql::ResultSet *resultSet, int numColumns)   
{ 
string builder = "";

   //Printing Records
   try
   {
      while (resultSet->next()) 
      {
         for (int i = 1; i <= numColumns; i++) 
         {
            if (i > 1)
               builder.append(",  ");
            builder.append(resultSet->getString(i)); 
         }
         builder.append("<br>");
      }
   }
   catch (sql::SQLException &e)
   {
   }
   return builder;
}

// Insert into any table, any values from data passed in as String parameters
void odbc_db::insert(string table, string values) 
{
string query = "INSERT into " + table + " values (" + values + ")";

   statement->executeUpdate(query);
}

// Remove all records and fill them with values for testing
// Assumes that the tables are already created
// Assumes that connection to database is already made
void odbc_db::initDatabase()
{
   try 
   {
      // Drop records from existing tables
      statement = connection->createStatement();
      statement->executeUpdate("DELETE from Student");
      statement->executeUpdate("DELETE from Course");
      statement->executeUpdate("DELETE from Enrollment");

      // Add records to the tables
      insert("Student", "101,'Andrew Brill','Chemical Engineering'");
      insert("Student", "102,'Erick Sands','Information System'");
      insert("Student", "103,'Kyle Key','Industrial Engineering'");
      insert("Student", "104,'Cy Starr','Mechanical Engineering'");
    //   insert("Student", "105,'Sarah Neoh','Biological Engineering'");
    //   insert("Student", "106,'Walter White','Chemistry'");
    //   insert("Student", "107,'Jesse Pinkman','Finance'");
    //   insert("Student", "108,'Ailin Zhang','Computer Science'");
    //   insert("Student", "109,'Kaitlyn Brinza','Mathmatics'");
    //   insert("Student", "110,'Dilendra Khanal','Computer Engineering'");
    //   insert("Student", "111,'Michael Panis','Computer Science'");
    //   insert("Student", "112,'Kory Trent','Electrical Engineering'");

      insert("Course", "'CSCE',2014,'Programming Foundations I',4");
      insert("Course", "'CSCE',4523,'Database Management Systems',3");
      insert("Course", "'MATH',2554,'Calculus I',4");
      insert("Course", "'MATH',3083,'Linear Algebra',3");
    

      insert("Enrollment", "101,'MATH',3083");
      insert("Enrollment", "101,'CSCE',2014");
      insert("Enrollment", "102,'MATH',2554");
      insert("Enrollment", "103,'MATH',3083");
      insert("Enrollment", "104,'CSCE',4523");


   }
   catch (sql::SQLException &e)
   {
      cout << "ERROR: SQLException in " << __FILE__;
      cout << " (" << __func__<< ") on line " << __LINE__ << endl;
      cout << "ERROR: " << e.what();
      cout << " (MySQL error code: " << e.getErrorCode();
      cout << ", SQLState: " << e.getSQLState() << ")" << endl;
   }
}
