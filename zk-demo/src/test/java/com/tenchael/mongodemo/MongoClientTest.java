package com.tenchael.mongodemo;

import com.mongodb.Block;
import com.mongodb.client.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.inc;

/**
 * Created by tengzhizhang on 2018/7/9.
 */
public class MongoClientTest extends Assert {


	@Test
	public void testGetDb() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017,localhost:27018");
		mongoClient.getDatabase("Article");

	}


	@Test
	public void testCreateCollection() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
		MongoDatabase database = mongoClient.getDatabase("Article");
		database.createCollection("test");
	}

	@Test
	public void testInsertDoc() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
		MongoDatabase database = mongoClient.getDatabase("Article");
		MongoCollection<Document> collection = database.getCollection("test");

		Document doc = new Document("name", "MongoDB")
				.append("type", "database")
				.append("count", 1)
				.append("versions", Arrays.asList("v3.2", "v3.0", "v2.6"))
				.append("info", new Document("x", 203).append("y", 102));
		collection.insertOne(doc);
	}

	@Test
	public void testInsertDocs() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
		MongoDatabase database = mongoClient.getDatabase("Article");
		MongoCollection<Document> collection = database.getCollection("test");

		List<Document> documents = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			documents.add(new Document("i", i));
		}

		collection.insertMany(documents);
	}


	@Test
	public void testCountDocs() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
		MongoDatabase database = mongoClient.getDatabase("Article");
		MongoCollection<Document> collection = database.getCollection("test");

		System.out.println(collection.countDocuments());
	}

	@Test
	public void testQuery() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
		MongoDatabase database = mongoClient.getDatabase("Article");
		MongoCollection<Document> collection = database.getCollection("test");

		Document myDoc = collection.find().first();
		System.out.println(myDoc.toJson());
	}

	@Test
	public void testFoudAll() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
		MongoDatabase database = mongoClient.getDatabase("Article");
		MongoCollection<Document> collection = database.getCollection("test");

		MongoCursor<Document> cursor = collection.find().iterator();
		try {
			while (cursor.hasNext()) {
				System.out.println(cursor.next().toJson());
			}
		} finally {
			cursor.close();
		}
	}

	@Test
	public void testFilter() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
		MongoDatabase database = mongoClient.getDatabase("Article");
		MongoCollection<Document> collection = database.getCollection("test");

		Document myDoc = collection.find(eq("i", 71)).first();
		System.out.println(myDoc.toJson());
	}

	@Test
	public void testFilter2() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
		MongoDatabase database = mongoClient.getDatabase("Article");
		MongoCollection<Document> collection = database.getCollection("test");

		Block<Document> printBlock = new Block<Document>() {
			@Override
			public void apply(final Document document) {
				System.out.println(document.toJson());
			}
		};

		collection.find(gt("i", 50)).forEach(printBlock);
	}

	@Test
	public void testUpdateDoc() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
		MongoDatabase database = mongoClient.getDatabase("Article");
		MongoCollection<Document> collection = database.getCollection("test");

		collection.updateOne(eq("i", 10), new Document("$set", new Document("i", 110)));
	}

	@Test
	public void testUpdateDocs() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
		MongoDatabase database = mongoClient.getDatabase("Article");
		MongoCollection<Document> collection = database.getCollection("test");

		UpdateResult updateResult = collection.updateMany(lt("i", 100), inc("i", 100));
		System.out.println(updateResult.getModifiedCount());
	}

	@Test
	public void testDelete() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
		MongoDatabase database = mongoClient.getDatabase("Article");
		MongoCollection<Document> collection = database.getCollection("test");

		DeleteResult deleteResult = collection.deleteOne(eq("i", 100));
		System.out.println(deleteResult.getDeletedCount());
	}

	@Test
	public void testDeleteMany() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
		MongoDatabase database = mongoClient.getDatabase("Article");
		MongoCollection<Document> collection = database.getCollection("test");

		DeleteResult deleteResult = collection.deleteMany(gte("i", 100));
		System.out.println(deleteResult.getDeletedCount());
	}

	@Test
	public void testCreateIndex() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
		MongoDatabase database = mongoClient.getDatabase("Article");
		MongoCollection<Document> collection = database.getCollection("test");

		//The following example creates an ascending index on the i field:
//		For an ascending index type, specify 1 for <type>.
//		For a descending index type, specify -1 for <type>.
		collection.createIndex(new Document("i", 1));
	}

}
