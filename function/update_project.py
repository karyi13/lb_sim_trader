import json
import os

def create_ladder_js():
    """读取 ladder_data.json 并生成 ladder_data.js"""
    print("正在处理: ladder_data.json -> ladder_data.js ...")
    json_path = 'ladder_data.json'
    js_path = 'ladder_data.js'
    
    if not os.path.exists(json_path):
        print(f"❌ 错误: 找不到源文件 {json_path}")
        return False

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 生成 JS 内容，赋值给 window.LADDER_DATA
        js_content = f"// 自动生成的连板数据文件\nwindow.LADDER_DATA = {json.dumps(data, ensure_ascii=False)};"
        
        with open(js_path, 'w', encoding='utf-8') as f:
            f.write(js_content)
        print(f"✅ 成功生成 {js_path} (大小: {os.path.getsize(js_path)/1024/1024:.2f} MB)")
        return True
    except Exception as e:
        print(f"❌ 生成 JS 文件失败: {e}")
        return False

def update_html():
    """更新 concept_ladder.html 移除硬编码数据并引入新 JS"""
    print("正在更新: concept_ladder.html ...")
    html_path = 'concept_ladder.html'
    
    if not os.path.exists(html_path):
        print(f"❌ 错误: 找不到文件 {html_path}")
        return

    try:
        with open(html_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        new_lines = []
        data_removed = False
        script_added = False
        
        for line in lines:
            stripped = line.strip()
            
            # 1. 插入 script 标签 (在 kline_data.js 后面)
            if 'src="kline_data.js"' in line and not script_added:
                new_lines.append(line)
                if 'src="ladder_data.js"' not in lines: # 防止重复添加
                    new_lines.append('    <script src="ladder_data.js"></script>\n')
                script_added = True
                continue
                
            # 2. 移除硬编码的大数据行
            if stripped.startswith('const actualData = {') or stripped.startswith('const actualData={'):
                new_lines.append('        // 数据已移至 ladder_data.js，通过 window.LADDER_DATA 获取\n')
                new_lines.append('        const actualData = window.LADDER_DATA || {};\n')
                new_lines.append('        if (Object.keys(actualData).length === 0) console.warn("警告: 未加载到连板数据");\n')
                data_removed = True
                print("✅ 已移除硬编码的 actualData 数据行")
                continue
            
            # 如果之前已经在 kline_data.js 处添加了，这里就不用管了
            # 如果还没有添加 script 且到了 body 结束，作为保底
            
            new_lines.append(line)

        # 写入文件
        if data_removed or script_added:
            with open(html_path, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)
            print("✅ HTML 更新成功！")
        else:
            print("⚠️ 未发现需要修改的内容，可能已经更新过。")

    except Exception as e:
        print(f"❌ 更新 HTML 失败: {e}")

if __name__ == "__main__":
    if create_ladder_js():
        update_html()