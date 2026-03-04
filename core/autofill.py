"""
智能补全与检索回填引擎 (Auto-Fill & Verification Module)
掌管了功能 [2]：利用之前入库的记忆，给那些只有一半源系统数据，却空着 SAP 字段的 Excel 行进行“填空”。
也是最核心的调度器：精确匹配 A -> 模糊查找 B -> 大模型兜底 C 的总司令部！
"""
import os
import glob
import openpyxl
import chromadb
from datetime import datetime
from openpyxl.styles import Alignment
from core.config import get_logger, get_script_dir
from core.excel_utils import find_headers, map_columns
from core.llm_service import evaluate_mapping_via_llm_batch

logger = get_logger(__name__)

def auto_fill_excel(file_path: str, sheet_name: str, db_path: str, collection_name: str):
    """读取一份带有空缺目标字段的 Excel 模板，利用多级漏斗去寻找答案并写回新文件。"""
    logger.info(f"开始为您指定的残缺表单进行智能填补: {file_path}")
    
    # 尝试连上记忆库
    client = chromadb.PersistentClient(path=db_path)
    collection = None
    try:
        # 尝试抽出那本叫做 collection_name 的大词典
        collection = client.get_collection(name=collection_name)
    except Exception as e:
        # 万一以前根本没选过 [1] 学习功能，直接抛个错。不要紧，它照旧会坚强地运行大模型C兜底功能。
        logger.warning(f"目前记忆库里没有内容 或 库没建好. 将全盘依赖大模型发威. 具体原因: {e}")

    # 开箱 Excel
    wb = openpyxl.load_workbook(file_path)
    if sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
    else:
        ws = wb.active

    # 找坐标，拿表头，定位源系统名称
    moto_cell, saki_cell = find_headers(ws)
    if not moto_cell or not saki_cell:
        logger.error("找不到模板对齐锚点。")
        return
        
    source_sys_name_cell = ws.cell(row=moto_cell.row + 1, column=moto_cell.column)
    source_sys_name = str(source_sys_name_cell.value).strip() if source_sys_name_cell.value else "未知源系统"

    header_row, col_src_desc, col_src_field, col_src_table, col_sap_desc, col_sap_table, col_sap_field = map_columns(ws, moto_cell, saki_cell)

    if None in (col_src_desc, col_src_field, col_sap_desc, col_sap_table, col_sap_field):
        logger.error("模版中残缺部分栏位")
        return

    # ======= 创建并定位：匹配来源 (A/B/C) 的标记列 =======
    col_match_source = None
    for cell in ws[header_row]:
        if cell.value in ["匹配结果来源", "匹配来源", "マッチ結果元", "マッチソース(A/B/C)", "マッチソース"]:
            col_match_source = cell.column
            break
            
    if not col_match_source:
        col_match_source = ws.max_column + 1
        ws.cell(row=header_row, column=col_match_source).value = "マッチ結果元"
    # ====================================================
    # 阶段零：收集全部目标
    # ====================================================
    candidates = []
    for row_idx in range(header_row + 1, ws.max_row + 1):
        src_field = str(ws.cell(row=row_idx, column=col_src_field).value or "").strip()
        src_desc = str(ws.cell(row=row_idx, column=col_src_desc).value or "").strip()
        src_table = str(ws.cell(row=row_idx, column=col_src_table).value or "").strip() if col_src_table else ""
        
        sap_table = ws.cell(row=row_idx, column=col_sap_table).value
        sap_field = ws.cell(row=row_idx, column=col_sap_field).value
        
        # 激活条件：只有左边有内容，必须且右边 SAP 是“空白”的，工具才会干活
        if src_field and src_desc and (not sap_table or not sap_field):
            doc_text = f"[source_system:{source_sys_name}] [source_table:{src_table}] [source_field:{src_field}] [source_description:{src_desc}]"
            candidates.append({
                "row_idx": row_idx,
                "src_field": src_field,
                "src_desc": src_desc,
                "src_table": src_table,
                "doc_text": doc_text
            })

    filled_count = 0
    vector_candidates = []
    
    # ===============================================================
    # 漏斗第一关：【A 级查询】精准匹配 (也就是 Metadata 的 Where 条件硬对齐)
    # 这一步属于本地轻量元数据查表，本身处于毫秒级，直接用 for 循环秒刷
    # ===============================================================
    if candidates and collection:
        logger.info(f"开启 A 级精确查询: {len(candidates)} 项待办...")
        for c in candidates:
            try:
                results = collection.query(
                    query_texts=[c["doc_text"]],
                    where={"$and": [
                        {"source_field_name": {"$eq": c["src_field"]}},
                        {"source_system_name": {"$eq": source_sys_name}}
                    ]},
                    n_results=1
                )
                
                # 命中了 A 级查询
                if results['ids'] and results['ids'][0]:
                    top_match = results['metadatas'][0][0]
                    # 直接在 Excel 当前行里盖戳！
                    ws.cell(row=c["row_idx"], column=col_sap_table).value = top_match['sap_table_name']
                    ws.cell(row=c["row_idx"], column=col_sap_field).value = top_match['sap_field_name']
                    if col_sap_desc:
                         ws.cell(row=c["row_idx"], column=col_sap_desc).value = top_match.get('sap_field_desc', '')
                    ws.cell(row=c["row_idx"], column=col_match_source).value = '完全一致'
                    
                    filled_count += 1
                    logger.info(f"Exact Match (级别 A) 命中: {c['src_field']} -> {top_match.get('sap_table_name')}.{top_match.get('sap_field_name')}")
                else:
                    vector_candidates.append(c)
            except Exception as e:
                logger.warning(f"发生故障放弃精确查询 '{c['src_field']}': {e}")
                vector_candidates.append(c)
    elif candidates and not collection:
        # 如果连记忆库都没有，跳过本地搜索直接给大模型兜底
        vector_candidates = candidates

    unmatched_for_llm = []
    
    # ===============================================================
    # 漏斗第二关：【批量 B 级查询】语义/维度坐标模糊相似度匹配 (Semantic/Similarity match)
    # 利用 ChromaDB 批量推理来节省计算所有语句的 Vector Embedding 耗时！
    # ===============================================================
    if vector_candidates and collection:
        logger.info(f"开启 B 级批量向量映射查询: {len(vector_candidates)} 项待办...")
        query_texts = [c["doc_text"] for c in vector_candidates]
        
        try:
             # 获取 TOP 3
             v_results = collection.query(
                 query_texts=query_texts,
                 n_results=3
             )
             
             for i, c in enumerate(vector_candidates):
                 if v_results['ids'] and v_results['ids'][i]:
                     valid_matches = []
                     for j in range(len(v_results['ids'][i])):
                         distance = v_results['distances'][i][j]
                         # 必须把误差卡死在 0.10，否则像“物料”会因为和“移动类型”在数据库里余弦角度差 0.12 而被当成近义词！宁缺毋滥，把不靠谱的交给大模型。
                         if distance < 0.10: 
                             valid_matches.append((distance, v_results['metadatas'][i][j]))
                     
                     if valid_matches: 
                         tables = [m['sap_table_name'] for d, m in valid_matches][:3]
                         fields = [m['sap_field_name'] for d, m in valid_matches][:3]
                         descs = [m.get('sap_field_desc', '') for d, m in valid_matches][:3]
                         
                         cell_table = ws.cell(row=c["row_idx"], column=col_sap_table)
                         cell_table.value = "\n".join(tables)
                         cell_table.alignment = Alignment(wrap_text=True)
                         
                         cell_field = ws.cell(row=c["row_idx"], column=col_sap_field)
                         cell_field.value = "\n".join(fields)
                         cell_field.alignment = Alignment(wrap_text=True)
                         
                         if col_sap_desc: 
                             cell_desc = ws.cell(row=c["row_idx"], column=col_sap_desc)
                             cell_desc.value = "\n".join(descs)
                             cell_desc.alignment = Alignment(wrap_text=True)
                             
                         ws.cell(row=c["row_idx"], column=col_match_source).value = 'ベクトル'
                         
                         filled_count += 1
                         logger.info(f"Vector Match (级别 B) 命中 {len(valid_matches)} 条: 首选 {c['src_field']} -> {valid_matches[0][1].get('sap_table_name')}.{valid_matches[0][1].get('sap_field_name')}")
                     else:
                         logger.info(f"库里搜到了 '{c['src_field']}' 近似词, 但误差全部 >= 0.10 未达标，直接丢给大模型.")
                         unmatched_for_llm.append(c)
                 else:
                     unmatched_for_llm.append(c)
        except Exception as e:
             logger.warning(f"发生故障放弃批量向量查询: {e}")
             unmatched_for_llm.extend(vector_candidates)
    elif vector_candidates and not collection:
        unmatched_for_llm = vector_candidates
                
    # ===============================================================
    # 最终关底：千问大脑线上【批量】推演 (LLM Batch Request)
    # 只要攒够了没做出来的题，一次性发给老师改卷，节约无数个 HTTP 网络开销时间！
    # ===============================================================
    if unmatched_for_llm:
        logger.info(f"正在将 {len(unmatched_for_llm)} 条未命中数据一并打包发给大模型推理...")
        
        # O(1) 的字典返回结果
        batch_results = evaluate_mapping_via_llm_batch(unmatched_for_llm)
        
        for item in unmatched_for_llm:
            r_idx = item["row_idx"]
            candidates = batch_results.get(r_idx, [])
            
            if candidates and isinstance(candidates, list) and len(candidates) > 0:
                tables = [c.get('sap_table_name', '') for c in candidates][:3]
                fields = [c.get('sap_field_name', '') for c in candidates][:3]
                descs = [c.get('sap_field_desc', '') for c in candidates][:3]
                
                cell_table = ws.cell(row=r_idx, column=col_sap_table)
                cell_table.value = "\n".join(tables)
                cell_table.alignment = Alignment(wrap_text=True)
                
                cell_field = ws.cell(row=r_idx, column=col_sap_field)
                cell_field.value = "\n".join(fields)
                cell_field.alignment = Alignment(wrap_text=True)
                
                if col_sap_desc: 
                    cell_desc = ws.cell(row=r_idx, column=col_sap_desc)
                    cell_desc.value = "\n".join(descs)
                    cell_desc.alignment = Alignment(wrap_text=True)
                    
                ws.cell(row=r_idx, column=col_match_source).value = 'AIモデル'

                filled_count += 1
                logger.info(f"Qwen 批量回填成功: 行 {r_idx} 提供了 {len(candidates)} 个候选结果。")
            else:
                ws.cell(row=r_idx, column=col_match_source).value = '未匹配'
                logger.info(f"Qwen 大模型也编造不出行 {r_idx} '{item['src_field']}'，宣告彻底放弃！")
    
    # 全部填写完毕！输出保存。
    if filled_count > 0:
        script_dir = get_script_dir()
        autofilled_folder = os.path.join(script_dir, "autofilled_output")
        os.makedirs(autofilled_folder, exist_ok=True)
        
        base, ext = os.path.splitext(os.path.basename(file_path))
        # 文件名打上当前年月日分秒的签，防止您把原模板被覆盖污染！非常安全。
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        new_filename = os.path.join(autofilled_folder, f"{base}_autofilled_{timestamp}{ext}")
        wb.save(new_filename)
        logger.info(f"大成功！成功用魔法填补了 {filled_count} 行！全新文件已安全落地: {new_filename}")
    else:
        logger.info("没有任何行被填上。（可能是没空缺可以填，或者三个级别全部战败没有搜到对应选项）。")

def process_autofill(path: str, sheet_name: str, db_path: str, collection_name: str):
    """一个小型路由器：用来决定用户丢进来的是一个文件字典文件夹结构，还是单独一个文件，然后依次下发去运行上方的 auto_fill。"""
    if os.path.isdir(path):
        excel_files = glob.glob(os.path.join(path, "*.xlsx"))
        for file in excel_files:
            if not os.path.basename(file).startswith("~") and not "_autofilled" in file:
                auto_fill_excel(file, sheet_name, db_path, collection_name)
    else:
        auto_fill_excel(path, sheet_name, db_path, collection_name)

def process_update_and_autofill(path: str, sheet_name: str, db_path: str, collection_name: str):
    """
    功能 [3] 综合模式：
    分两阶段运行。
    第一段：扫描所有表结构里，那些有SAP目标映射值的，先扒出来吃进肚子里背掉（学习ingest）；
    第二段：转身去把刚才同一批表中空着一半没SAP结果的行，调用刚刚入库背好的新鲜记忆，以及大模型尝试再填上(自动生成)。
    """
    from core.ingestion import process_ingest
    if os.path.isdir(path):
        excel_files = glob.glob(os.path.join(path, "*.xlsx"))
        # Phase 1: Ingest all available knowledge first (知识沉淀)
        for file in excel_files:
            if not os.path.basename(file).startswith("~") and not "_autofilled" in file:
                process_ingest(file, sheet_name, db_path, collection_name)
        # Phase 2: Backfill any gaps using the newly enriched DB (大反攻：回填空缺)
        for file in excel_files:
            if not os.path.basename(file).startswith("~") and not "_autofilled" in file:
                auto_fill_excel(file, sheet_name, db_path, collection_name)
    else:
        # Phase 1
        process_ingest(path, sheet_name, db_path, collection_name)
        # Phase 2
        auto_fill_excel(path, sheet_name, db_path, collection_name)
