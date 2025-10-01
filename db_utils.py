# db_utils.py
# 数据库工具模块，提供用户数据管理功能

import os
import json
import sqlite3
import threading
from contextlib import closing
from datetime import datetime

# 数据库路径 - 默认使用绘影租户数据库
DB_PATH = 'user-700243.db'
USERS_FILE = 'users.json'

# 数据库锁
db_lock = threading.Lock()

def init_db():
    """初始化数据库，创建必要的表结构"""
    if not os.path.exists(DB_PATH):
        with closing(sqlite3.connect(DB_PATH)) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                )
            ''')
            conn.commit()

def load_users():
    """
    加载用户数据
    优先从数据库加载，如果数据库为空则从JSON文件加载
    """
    users = {}
    
    # 尝试从数据库加载
    if os.path.exists(DB_PATH):
        try:
            with closing(sqlite3.connect(DB_PATH)) as conn:
                cursor = conn.execute('SELECT username, data FROM users')
                for row in cursor.fetchall():
                    username, data_json = row
                    try:
                        users[username] = json.loads(data_json)
                    except json.JSONDecodeError:
                        continue
        except sqlite3.Error:
            pass
    
    # 如果数据库为空，尝试从JSON文件加载
    if not users and os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                users = data.get('users', {})
        except (json.JSONDecodeError, FileNotFoundError):
            pass
    
    return users

def save_users(users):
    """
    保存用户数据到数据库和JSON文件
    """
    with db_lock:
        # 保存到数据库
        if os.path.exists(DB_PATH) or True:  # 总是尝试保存到数据库
            try:
                with closing(sqlite3.connect(DB_PATH)) as conn:
                    # 清空现有数据
                    conn.execute('DELETE FROM users')
                    
                    # 插入新数据
                    for username, user_data in users.items():
                        data_json = json.dumps(user_data, ensure_ascii=False)
                        conn.execute('INSERT INTO users (username, data) VALUES (?, ?)', 
                                   (username, data_json))
                    
                    conn.commit()
            except sqlite3.Error as e:
                print(f"数据库保存失败: {e}")
        
        # 同时保存到JSON文件作为备份
        try:
            with open(USERS_FILE, 'w', encoding='utf-8') as f:
                json.dump({'users': users}, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"JSON文件保存失败: {e}")

def get_db_path():
    """获取当前数据库路径"""
    return DB_PATH

def set_db_path(path):
    """设置数据库路径（用于多租户支持）"""
    global DB_PATH
    DB_PATH = path