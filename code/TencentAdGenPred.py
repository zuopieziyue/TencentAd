#coding=utf-8
import random
import pandas as pd
import time, datetime

test_file = "D:\\github\\TencentAd\\data\\algo.qq.com_641013010_testa\\testA\\test_sample.dat"
pred_file = "submission.csv"

#=====================================================
#生成预测文件，随机生成和规则生成
#=====================================================
#随机生成测试文件，只在第一次提交时使用，测试提交过程
def gen_pred_file_random(test_file, pred_file):
	f1 = open(test_file, 'r')
	f2 = open(pred_file, 'w')
	for line in f1:
		line = line.strip().split("\t")
		#生成随机数，浮点类型
		a = random.uniform(0, 100)
		f2.write(str(line[0]) + "," + str(round(a, 4)) + "\n")

	f1.close()
	f2.close()

#规则生成预测文件
def gen_pred_file_rule(test_file, pred_file):
	f1 = open(test_file, 'r')
	f2 = open(pred_file, 'w')
	idList = []
	bidList = []
	for line in f1:
		line = line.strip().split("\t")
		idList.append(line[0])
		bidList.append(float(line[-1]))
	f1.close()

	obj = pd.Series(bidList)
	obj = obj.rank()
	for i in range(len(bidList)):
		tmp = bidList[i] / obj[i]
		f2.write(idList[i] + "," + str(tmp) + "\n")
	f2.close()


if __name__ == "__main__":
	#====================================================
	gen_pred_file_random(test_file, pred_file)
	gen_pred_file_rule(test_file, pred_file)
