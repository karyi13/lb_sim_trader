import os

file_path = 'concept_ladder.html'
backup_path = 'concept_ladder.html.bak'

try:
    # 读取文件
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # 创建备份
    with open(backup_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    print(f"已创建备份文件: {backup_path}")

    new_lines = []
    found = False
    for line in lines:
        # 查找包含硬编码数据的行 (以 const actualData = {" 开头)
        if line.strip().startswith('const actualData = {"'):
            # 替换为从全局变量获取，增加了兼容性检查
            new_lines.append('        // 数据已通过 kline_data.js 或其他外部文件加载\n')
            new_lines.append('        const actualData = window.actualData || window.LADDER_DATA || {};\n')
            new_lines.append('        if (Object.keys(actualData).length === 0) console.warn("警告: 未检测到 actualData 数据，请检查 kline_data.js 是否正确加载");\n')
            found = True
            print("已找到并替换硬编码的 actualData 数据。")
        else:
            new_lines.append(line)

    if found:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
        print("concept_ladder.html 更新成功！")
    else:
        print("未找到需要替换的数据行，请检查文件内容是否已被修改。")

except Exception as e:
    print(f"发生错误: {e}")