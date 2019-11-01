#coding=utf-8
import random
import pandas as pd
import time, datetime

total_exposure_log_file = "D:\\github\\TencentAd\\data\\algo.qq.com_641013010_testa\\testA\\totalExposureLog.out"
user_data_file = "D:\\github\\TencentAd\\data\\algo.qq.com_641013010_testa\\testA\\user_data"
ad_static_feature_file = "D:\\github\\TencentAd\\data\\algo.qq.com_641013010_testa\\testA\\ad_static_feature.out"
ad_operation_file = "D:\\github\\TencentAd\\data\\algo.qq.com_641013010_testa\\testA\\ad_operation.dat"

test_file = "D:\\github\\TencentAd\\data\\algo.qq.com_641013010_testa\\testA\\test_sample.dat"
pred_file = "submission.csv"

total_exposure_log_sample_file = "D:\\github\\TencentAd\\data\\totalExposureLogSample.out"
user_data_count_file = "D:\\github\\TencentAd\\data\\user_data_count"

total_exposure_log_sample_dfile = "D:\\github\\TencentAd\\data\\totalExposureLogSample.csv"
user_data_count_dfile = "D:\\github\\TencentAd\\data\\user_data_count.csv"
ad_static_feature_dfile = "D:\\github\\TencentAd\\data\\ad_static_feature.csv"
ad_operation_dfile = "D:\\github\\TencentAd\\data\\ad_operation.csv"

train_dfile = "D:\\github\\TencentAd\\data\\train.csv"
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

#=====================================================
#处理数据文件
#=====================================================
#对历史曝光数据文件进行采样，既然训练数据本来也是采样来的，那么再次采样道理上应该是行的通的
def gen_total_exposure_log_sample(total_exposure_log_file, total_exposure_log_sample_file):
	f1 = open(total_exposure_log_file, 'r')
	f2 = open(total_exposure_log_sample_file, 'w')
	i = 0
	for line in f1:
		if i%5 == 0:
			f2.write(line)
		i = i + 1
	
	f1.close()
	f2.close()

#将用户属性文件中的多值进行count处理，减小文件
def gen_user_data_count_file(user_data_file, user_data_count_file):
	f1 = open(user_data_file, 'r')
	f2 = open(user_data_count_file, 'w')
	i = 0
	for line in f1:
		line = line.strip().split("\t")
		if len(line) != 11:
			continue

		#处理地域
		area = str(len(line[3].split(',')))
		line[3] = area

		#处理婚恋状态
		status = str(len(line[4].split(',')))
		line[4] = status

		#工作状态
		work = str(len(line[8].split(',')))
		line[8] = work

		#
		behavior = str(len(line[10].split(',')))
		line[10] = behavior

		f2.write('\t'.join(line) + '\n')

		i += 1
		if i % 10000 == 0:
			print i

	f1.close()
	f2.close()


#=====================================================
#生成csv文件
#=====================================================
def gen_csv_file():
	print ("gen total_exposure_log_sample_dfile")
	total_exposure_log_sampledf = pd.read_table(total_exposure_log_sample_file,header=None, 
			names=['adreqid','adreqtime','adlocalid','userid','adid','materialsize','bid','pctr','qualityecpm','totalecpm'])
	total_exposure_log_sampledf.to_csv(total_exposure_log_sample_dfile)

	print ("gen user_data_count_dfile")
	user_data_countdf = pd.read_table(user_data_count_file,header=None, 
			names=['userid','age','gender','area','status','education','consuptionability','device','work','connectiontype','behavior'])
	user_data_countdf.to_csv(user_data_count_dfile)

	print ("gen ad_static_feature_dfile")
	ad_static_featuredf = pd.read_table(ad_static_feature_file,header=None, 
			names=['adid','createtime','adaccountid','goodsid','goodstype','adbusiid','materialsize'])
	ad_static_featuredf.to_csv(ad_static_feature_dfile)

	print ("gen ad_operation_dfile")
	ad_operationdf = pd.read_table(ad_operation_file,header=None, 
			names=['adid','optime','optype','modifyfield','fieldvalue'])
	ad_operationdf.to_csv(ad_operation_dfile)

#=====================================================
#生成训练集
#=====================================================
def gen_train_file():
	total_exposure_log_sampledf = pd.read_csv(total_exposure_log_sample_dfile, low_memory=True)

	train

	print total_exposure_log_sampledf.adid.value_counts()



##历史曝光数据文件分解为小文件，按日期读取
#def gen_total_exposure_log_sample(total_exposure_log_file, total_exposure_log_sample_file):
#	f1 = open(total_exposure_log_file, 'r')
#	f2 = open(total_exposure_log_sample_file, 'w')
#	i = 0
#	dateList = []
#	for line in f1:
#		#替换为时间格式
#		line = line.strip().split('\t')
#		timeStamp = float(line[1])
#		timeArray = time.localtime(timeStamp)
#		otherStyleTime = time.strftime("%Y%m%d%H%M%S", timeArray)
#		line[1] = otherStyleTime
#		line = '\t'.join(line)
#		
#		#按日期分解文件
#		if otherStyleTime[:8] not in dateList:
#			dateList.append(otherStyleTime[:8])
#			f2.close
#
#		if i == 10000:
#			break
#		f2.write(line + '\n')
#		i = i + 1
#
#	
#	f1.close()
#	f2.close()




#对采样后的历史曝光数据文件groupby，计算曝光
def groupby_total_exposure_log_sample(total_exposure_log_sample_file, exposure_log_sample_groupby_file):
	total_exposure_log_sample = pd.read_table(total_exposure_log_sample_file,header=None, names=['a','b','c','d','e','f','g','h','i','j'])
	exposure_log_sample_groupby = total_exposure_log_sample['j'].groupby(total_exposure_log_sample['a']).sum()
	#f1 = open(exposure_log_sample_groupby_file, 'w')
	#i = 0
	#for i in range(len(exposure_log_sample_groupby['a'])):
	#	line = '\t'.join(exposure_log_sample_groupby[i])
	#	f1.write(line + '\n')
	#
	#	if i%10000 == 0:
	#		print i
	#
	#f1.close()

	exposure_log_sample_groupby.to_csv(exposure_log_sample_groupby_file)









if __name__ == "__main__":
	#====================================================
	#gen_pred_file_random(test_file, pred_file)
	#gen_pred_file_rule(test_file, pred_file)
	
	#====================================================
	#gen_total_exposure_log_sample(total_exposure_log_file, total_exposure_log_sample_file)
	#gen_user_data_count_file(user_data_file, user_data_count_file)

	#====================================================
	#gen_csv_file()

	#====================================================
	gen_train_file()



	#groupby_total_exposure_log_sample(total_exposure_log_sample_file, exposure_log_sample_groupby_file)
	
	




