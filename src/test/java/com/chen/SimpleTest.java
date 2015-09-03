package com.chen;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.Block;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.result.DeleteResult;

public class SimpleTest {
	//1.完成加简单MongoDB的数据库操作
	//使用空的构造方法，将不会自动发现
	//MongoClient mongoClient = new MongoClient();
	
	// or
	//MongoClient mongoClient = new MongoClient( "localhost" );

	// or
	//private MongoClient mongoClient = new MongoClient( "localhost" , 27017 );

	// or, to connect to a replica set, with auto-discovery of the primary, supply a seed list of members
	/*MongoClient mongoClient = new MongoClient(
	  Arrays.asList(new ServerAddress("localhost", 27017),
	                new ServerAddress("localhost", 27018),
	                new ServerAddress("localhost", 27019)));
*/
	// or use a connection string
	/*MongoClientURI connectionString = new MongoClientURI("mongodb://localhost:27017,localhost:27018,localhost:27019");
	MongoClient mongoClient = new MongoClient(connectionString);*/

	private MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	//getDatabase中的test为MongoDB中要使用的数据库，这里我们使用 test
	private MongoDatabase database = mongoClient.getDatabase("test");
	
	/**
	 * 
	 */
	@Test
	public void testInsert(){
		
		//获取Collection对象 getCollection("test")的test为字段
		MongoCollection<Document> collection = database.getCollection("test");
		Document document = new Document("name","mongodb")
					.append("type", "database")
					.append("count", 1)
					.append("info", new Document("x",203).append("y", 102));
		collection.insertOne(document);
	}
	
	@Test
	public void testManyInsert(){
		MongoCollection<Document> collection = database.getCollection("manyTest");
		List<Document> documents = new ArrayList<Document>();
		for(int i = 0; i < 10; i++){
			documents.add(new Document("i",i));
		}
		collection.insertMany(documents);
	}
	
	@Test
	public void testQuery(){
		MongoCollection<Document> collection = database.getCollection("manyTest");
		System.out.println("collection count : " + collection.count());
		Document myDocument = collection.find().first();
		System.out.println("collection Json : " + myDocument.toJson());
	}
	
	@Test
	public void testFindAll(){
		MongoCollection<Document> collection = database.getCollection("manyTest");
		System.out.println("collection count : " + collection.count());
		MongoCursor<Document> cursor = collection.find().iterator();
		
		try{
			while(cursor.hasNext()){
				System.out.println(cursor.next().toJson());
			}
		}finally{
			cursor.close();
		}
	}
	
	/**
	 * 通过条件查询
	 */
	@Test
	public void queryFilter(){
		MongoCollection<Document> collection = database.getCollection("manyTest");
		Document myDocument = collection.find(Filters.eq("i", 1)).first();
		System.out.println("myDocument : " + myDocument.toJson());
		
	}
	
	@Test
	public void subSetQuery(){
		MongoCollection<Document> collection = database.getCollection("manyTest");
		// now use a range query to get a larger subset
		Block<Document> printBlock = new Block<Document>() {
		     public void apply(final Document document) {
		         System.out.println(document.toJson());
		     }
		};
		collection.find(Filters.lt("i", 6)).forEach(printBlock);
	}
	
	
	/**
	 * 对查找出来的结果进行排序
	 */
	@Test
	public void sort(){
		
		Block<Document> printBlock = new Block<Document>() {

			public void apply(final Document document) {
				System.out.println(document.toJson());
			}
		};
		MongoCollection<Document> collection = database.getCollection("manyTest");
		collection.find().sort(Sorts.descending("i")).forEach(printBlock);;
	}
	
	/**
	 * Projectiong fileds
	 * 有时候我们并不需要所有的数据包含在文档中Projections可以帮助我们
	 * 构建一个查找的操作。通过本方法，在查找中取出_id
	 */
	@Test
	public void testProjections(){
		MongoCollection<Document> collection = database.getCollection("manyTest");
		Block<Document> printBlock = new Block<Document>() {
			public void apply(Document t) {
				System.out.println(t.toJson());
			}
		};
		collection.find().projection(Projections.excludeId())
						 .forEach(printBlock);
	}
	
	
	/*
	 * 修改 把i = 5的一项修改为i = 110
	 */
	@Test
	public void testUpdate(){
		MongoCollection<Document> collection = database.getCollection("manyTest");
		collection.updateOne(Filters.eq("i", 5), new Document("$set", new Document("i", 110)));
	}
	
	/**
	 * 删除操作
	 */
	@Test
	public void testDelete(){
		MongoCollection<Document> collection = database.getCollection("manyTest");
		//删除i = 0的一项
		DeleteResult deleteResult = collection.deleteOne(Filters.eq("i", 0));
		System.out.println("deleteCount: " + deleteResult.getDeletedCount());
	}
	
}