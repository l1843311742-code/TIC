import os
import chromadb

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "vector_store")
    collection_name = "mapping_collection"

    print(f"正在尝试连接向量数据库: {db_path}...")
    try:
        client = chromadb.PersistentClient(path=db_path)
        collection = client.get_collection(name=collection_name)
        
        # 获取集合里所有的数据，明确要求包含高维向量 (embeddings)
        data = collection.get(include=['documents', 'metadatas', 'embeddings'])
        
        total_count = len(data['ids'])
        print(f"数据库中当前包含 {total_count} 条记忆映射记录！\n")
        
        if total_count == 0:
            print("目前数据库是空的，请先通过菜单 [1] 导入学习数据。")
            return

        for i in range(total_count):
            print("-" * 60)
            print(f"📌 [记录 {i+1}] ID: {data['ids'][i]}")
            print(f"📝 学习的原始文本: {data['documents'][i]}")
            
            # 显示高维向量数字（像您截图中那样的 [0.032..., 0.027...]）
            if data.get('embeddings') is not None and len(data['embeddings']) > i:
                emb = data['embeddings'][i]
                # 为了防止满屏数字，只展示前 5 个维度的坐标和小数点
                emb_preview = ",".join([f"{x:.6f}" for x in emb[:5]]) + ", ..."
                print(f"🔢 底层数学向量 (Embeddings): [{emb_preview}] (共 {len(emb)} 维)")
            
            metadata = data['metadatas'][i]
            sap_table = metadata.get('sap_table_name', '无')
            sap_field = metadata.get('sap_field_name', '无')
            print(f"🎯 绑定的SAP对应关系: 结构=>{sap_table}  名称=>{sap_field}")
            
        print("-" * 60)
            
    except Exception as e:
        print(f"\n无法读取数据库或集合尚未创建，具体报错: {e}")
        print("这可能意味着您还没有执行过模式 [1] 学习功能，或者数据库路径已经被移动。")

if __name__ == "__main__":
    main()
