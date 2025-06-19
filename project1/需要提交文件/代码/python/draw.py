import matplotlib.pyplot as plt

# 数据
methods = [
    'loader1', 'loader2', 'loader3', 'loader4', 'loader5',
    'loader6', 'loader6_notrigger', 'loader7', 'loader7_notrigger'
]
values = [0, 11576, 13197, 30466, 69047, 73102, 106240, 160037, 216646]

# 画图
plt.figure(figsize=(10, 6))
plt.bar(methods, values, color='skyblue')
plt.xlabel("导入方式")
plt.ylabel("值")
plt.title("不同导入方式的值比较")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
