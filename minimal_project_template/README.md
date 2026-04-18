# Minimal Project Template

适合下面这类项目直接复用：

- Windows + PowerShell 环境下经常直接查看 `md` / `txt`
- Python 脚本会读取简单的 `key=value` 配置文件
- 希望统一编码、换行和 Git 忽略规则

## 包含内容

- `.editorconfig`
- `.gitignore`
- `.gitattributes`
- `config.example.txt`
- `python/config_loader.py`

## 推荐用法

1. 把 `.editorconfig`、`.gitignore`、`.gitattributes` 复制到新项目根目录
2. 把 `python/config_loader.py` 放到新项目的公共工具目录
3. 把 `config.example.txt` 改成你的实际配置文件
4. 文档、日志、参数文件优先保存为 `UTF-8 with BOM`

## 为什么这样配

- `md` / `txt` 使用 `UTF-8 with BOM`
  这样在 Windows PowerShell 里直接 `Get-Content` 读取中文更稳，不容易乱码
- Python 代码仍保持普通 `utf-8`
  兼容性和跨平台习惯更好
- 配置读取用 `utf-8-sig`
  即使文本文件带 BOM，也不会把 BOM 当成首个字符读进配置键名

## Python 示例

```python
from pathlib import Path

from python.config_loader import load_key_value_config

config_path = Path(__file__).with_name("config.txt")
config = load_key_value_config(config_path)

print(config["project_name"])
```

## 可选项

- 如果你的团队完全不用 PowerShell 直接读中文文档，可以把 `md` / `txt` 的编码改回普通 `utf-8`
- 如果项目里有大量二进制产物，可以继续补充 `.gitattributes` 和 `.gitignore`
