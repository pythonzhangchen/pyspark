# -*- coding: utf-8 -*-
# @Time : 2022/9/9 22:54 
# @Author : chen.zhang 
# @File : jieba.py

import jieba

from defs import filter_words

if __name__ == '__main__':
    content = '小明硕士毕业于中国科学院计算所后再清华大学深造'

    result = jieba.cut(content, True)
    print(list(result))
    print(type(result))

    result2= jieba.cut(content,False)
    print(list(result2))

    # 用搜索引擎模式，等同于允许二次组合的场景
    result3 =jieba.cut_for_search(filter_words)
    print(",".join(result3))

