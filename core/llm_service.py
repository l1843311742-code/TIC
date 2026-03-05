"""
大模型专属通讯模块 (LLM Fallback Service)
当前两关（精确数据库查找A，向量相似查找B）全部失败，什么都没搜到或者匹配度不够时，
这个模块就会扮演最后的兜底角色法宝（C），把悬念交给阿里千问的推理大脑完成智能推理！
"""
import os
import json
from core.config import get_logger

logger = get_logger(__name__)

def evaluate_mapping_via_llm_batch(unmatched_items: list) -> dict:
    """
    封装了阿里千问 Qwen 模型，支持并行批量推断。
    传入 [{'row_idx': x, 'src_field': 'xxx', 'src_desc': 'yyy'}, ...]
    传出 {row_idx: {'sap_table_name': '...', 'sap_field_name': '...', 'sap_field_desc': '...'}, ...}
    """
    if not unmatched_items:
        return {}
        
    import dashscope
    import concurrent.futures
    dashscope.api_key = os.environ.get("DASHSCOPE_API_KEY")
    
    if not dashscope.api_key:
        logger.warning("LLM APIキー(DASHSCOPE_API_KEY)が見つかりません。バッチAI推論処理をスキップします。")
        return {}

    sys_prompt = """You are an elite SAP Data Migration and Integration Expert. I will provide you with a JSON array containing custom/source system fields, each with a unique 'row_idx'.
For each item, predict EXACTLY 3 corresponding standard SAP Table Names and Field Names (e.g., MARA/MATNR, VBAK/VBELN), ranked by your confidence level from highest to lowest.

RULES FOR ACCURACY:
1. Strongly bias towards standard SAP Modules (SD, MM, FI, CO, PP, etc.) and their primary tables.
2. If the source field indicates a primary key (e.g., Order No, Material No), map it to the header/item table keys.
3. If the source field description is Japanese/Chinese, use your multilingual SAP dictionary knowledge to find the exact German/English abbreviation for the SAP field.

Your response MUST be a VALID JSON ARRAY.
Do not wrap it in markdown block quotes (do not use ```json), just output the raw JSON array.
Each object in the array MUST contain:
- "row_idx": (must match exactly the row_idx provided)
- "candidates": A JSON array containing exactly 1 to 3 objects, sorted from highest confidence to lowest. 
Each candidate object MUST contain:
   - "sap_table_name"
   - "sap_field_name"
   - "sap_field_desc" (must be translated to the exact same language as the source description)
   - "score" (a float between 0.01 and 0.99 indicating confidence)
If you cannot find any mappings, return an empty array for "candidates"."""

    result_map = {}
    chunk_size = 5  # 每个批次5个字段，避免单次请求过大导致超时
    total_chunks = (len(unmatched_items) + chunk_size - 1) // chunk_size
    
    logger.info(f"AIモデル処理開始: 合計 {len(unmatched_items)} 件を {total_chunks} バッチに分割（各バッチ最大{chunk_size}件）")

    def fetch_chunk(chunk_index, chunk_data):
        """单个批次的API调用"""
        payload = [{"row_idx": item["row_idx"], "src_field": item["src_field"], "src_desc": item["src_desc"]} for item in chunk_data]
        user_prompt = f"Batch payload: {json.dumps(payload, ensure_ascii=False)}"
        
        logger.info(f"📤 バッチ {chunk_index + 1}/{total_chunks} 送信中 ({len(chunk_data)}件)...")

        try:
            import time
            start_time = time.time()
            
            response = dashscope.Generation.call(
                model='qwen-max',
                messages=[
                    {'role': 'system', 'content': sys_prompt},
                    {'role': 'user', 'content': user_prompt}
                ],
                result_format='message',
                timeout=120  # 120秒超时
            )
            
            elapsed = time.time() - start_time

            if response.status_code == 200:
                content = response.output.choices[0]['message']['content']
                if "```json" in content:
                    content = content.split("```json")[-1].split("```")[0].strip()
                elif "```" in content:
                    content = content.split("```")[-1].split("```")[0].strip()
                
                result = json.loads(content)
                logger.info(f"✅ バッチ {chunk_index + 1}/{total_chunks} 完了 (耗时: {elapsed:.2f}秒, {len(result)}件返回)")
                return result
            else:
                logger.error(f"❌ バッチ {chunk_index + 1} 失敗: {response.code} {response.message}")
                return []
                
        except Exception as llm_e:
            logger.warning(f"⚠️  バッチ {chunk_index + 1} エラー: {llm_e}")
            return []

    # 使用线程池并发调用API（IO密集型任务适合多线程）
    chunks = [unmatched_items[i:i + chunk_size] for i in range(0, len(unmatched_items), chunk_size)]
    
    # 并发数设置：5个线程同时调用API
    max_workers = 5
    logger.info(f"🚀 {max_workers}個の並列スレッドでAPI呼び出しを開始...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        futures = {executor.submit(fetch_chunk, idx, chunk): idx for idx, chunk in enumerate(chunks)}
        
        # 等待完成并收集结果
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            completed += 1
            predictions = future.result()
            
            # 处理返回的结果
            for pred in predictions:
                idx = pred.get("row_idx")
                if idx is not None:
                    result_map[idx] = pred.get("candidates", [])
            
            # 显示总体进度
            logger.info(f"📊 進捗: {completed}/{total_chunks} バッチ完了 ({completed*100//total_chunks}%)")
    
    logger.info(f"✨ AIモデル処理完了: {len(result_map)} 件の結果を取得")
    return result_map
