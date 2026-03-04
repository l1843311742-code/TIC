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
    dashscope.api_key = os.environ.get("DASHSCOPE_API_KEY")
    
    if not dashscope.api_key:
        logger.warning("没有找到千问的大模型密钥(DASHSCOPE_API_KEY)，自动跳过批量C环节大模型求助。")
        return {}

    sys_prompt = """You are an SAP mapping expert. I will provide you with a JSON array containing multiple Source System fields, each with a unique 'row_idx'.
For each item, predict UP TO 3 corresponding SAP Table Names and Field Names, ranked by your confidence level.
Your response MUST be a VALID JSON ARRAY.
Do not wrap it in markdown block quotes (do not use ```json), just output the raw JSON array.
Each object in the array MUST contain:
- "row_idx": (must match exactly the row_idx provided)
- "candidates": A JSON array containing 1 to 3 objects, sorted from highest confidence to lowest. 
Each candidate object MUST contain:
   - "sap_table_name"
   - "sap_field_name"
   - "sap_field_desc" (must be translated to the exact same language as the source description)
   - "score" (a float between 0.01 and 0.99 indicating confidence)
If you cannot find any mappings, return an empty array for "candidates"."""

    # 只丢进模型需要推理的有效干货以减少 Token 消耗
    payload = [{"row_idx": item["row_idx"], "src_field": item["src_field"], "src_desc": item["src_desc"]} for item in unmatched_items]
    user_prompt = f"Batch payload: {json.dumps(payload, ensure_ascii=False)}"

    try:
        response = dashscope.Generation.call(
            model='qwen-plus',
            messages=[
                {'role': 'system', 'content': sys_prompt},
                {'role': 'user', 'content': user_prompt}
            ],
            result_format='message'
        )

        if response.status_code == 200:
            content = response.output.choices[0]['message']['content']
            
            # 清理模型的 Markdown 输出癖好
            if "```json" in content:
                content = content.split("```json")[-1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[-1].split("```")[0].strip()
                
            predictions = json.loads(content)
            
            # 整理成字典以供 O(1) 快速查询回填: {row_idx: [ { sap_... }, { sap_... } ], row_idx2: [ ... ]}
            result_map = {}
            for pred in predictions:
                idx = pred.get("row_idx")
                if idx is not None:
                    # 获取最多3个候选结果并原样传出
                    candidates = pred.get("candidates", [])
                    result_map[idx] = candidates
            return result_map
        else:
            logger.error(f"大模型批量 API 求助失败: {response.code} {response.message}")
            return {}
            
    except Exception as llm_e:
        logger.warning(f"大模型批量兜底发生灾难性故障，本批次放弃: {llm_e}\nQwen Output was: {content if 'content' in locals() else 'None'}")
        return {}
