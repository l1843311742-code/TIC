import os
import zipfile
import xml.etree.ElementTree as ET

def extract_text_from_docx(docx_path):
    """Simple DOCX text extractor avoiding external dependencies like python-docx."""
    if not os.path.exists(docx_path):
        return f"File not found: {docx_path}"
        
    try:
        with zipfile.ZipFile(docx_path) as docx:
            xml_content = docx.read('word/document.xml')
        
        tree = ET.XML(xml_content)
        
        # XML namespace for word processing
        WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
        PARA = WORD_NAMESPACE + 'p'
        TEXT = WORD_NAMESPACE + 't'
        
        paragraphs = []
        for paragraph in tree.iter(PARA):
            texts = [node.text for node in paragraph.iter(TEXT) if node.text]
            if texts:
                paragraphs.append(''.join(texts))
                
        return '\n'.join(paragraphs)
    except Exception as e:
        return f"Error extracting {docx_path}: {e}"

if __name__ == "__main__":
    file1 = r"D:\antigravity\TIC\file\存量资产提取工具设计.docx"
    file2 = r"D:\antigravity\TIC\file\IF MAPPING工具设计.docx"
    
    with open("docs_content.txt", "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write(f"--- CONTENT OF {file1} (FUNCTION 1) ---\n")
        f.write(extract_text_from_docx(file1) + "\n")
        
        f.write("\n" + "=" * 60 + "\n")
        f.write(f"--- CONTENT OF {file2} (FUNCTION 2) ---\n")
        f.write(extract_text_from_docx(file2) + "\n")
        f.write("=" * 60 + "\n")
    print("Done")
