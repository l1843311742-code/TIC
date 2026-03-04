import chromadb

# Test ChromaDB connection
try:
    client = chromadb.PersistentClient(path="./vector_store")
    print("✅ ChromaDB client created successfully")
    
    # Try to delete existing collection if it exists
    try:
        client.delete_collection("test_collection")
        print("✅ Existing collection deleted")
    except:
        pass
    
    # Create a test collection
    collection = client.create_collection(
        name="test_collection",
        metadata={"hnsw:space": "cosine"}
    )
    print("✅ Collection created successfully")
    
    # Add test data
    collection.add(
        ids=["1"],
        metadatas=[{"source_system_name": "test", "source_field_name": "test_field"}],
        documents=["Test document"],
        embeddings=[[0.0]*128]
    )
    print("✅ Data added successfully")
    
    # Query test data
    results = collection.query(
        query_embeddings=[[0.0]*128],
        where={"$and": [
            {"source_field_name": {"$eq": "test_field"}},
            {"source_system_name": {"$eq": "test"}}
        ]},
        n_results=1
    )
    print("✅ Query executed successfully")
    print(f"Results: {results}")
    
    # Delete test collection
    client.delete_collection("test_collection")
    print("✅ Test collection deleted")
    
    print("\n🎉 All ChromaDB operations completed successfully!")
    
except Exception as e:
    print(f"❌ Error: {e}")
