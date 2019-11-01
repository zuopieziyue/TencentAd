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

total_exposure_log_dfile = "D:\\github\\TencentAd\\data\\totalExposureLog.csv"
ad_static_feature_dfile = "D:\\github\\TencentAd\\data\\ad_static_feature.csv"
ad_operation_dfile = "D:\\github\\TencentAd\\data\\ad_operation.csv"

datapath = "D:\\github\\TencentAd\\data\\"


train_dfile = "D:\\github\\TencentAd\\data\\train.csv"


#=====================================================
#该模块主要对数据进行预处理，并生成训练集和测试集
#=====================================================
#广告静态数据，生成csv文件，并清洗数据
def pre_ad_static_feature():
	ad_static_featuredf = pd.read_table(ad_static_feature_file,header=None, 
			names=['adid','createtime','adaccountid','goodsid','goodstype','adbusiid','materialsize'], low_memory=True)

	#删除创建时间为0的异常数据
	ad_static_featuredf = ad_static_featuredf[~ad_static_featuredf['createtime'].isin([0])]
	#删除素材尺寸为空的异常数据
	ad_static_featuredf = ad_static_featuredf.dropna(axis = 0)
	#删除广告行业id为多值的情况
	ad_static_featuredf = ad_static_featuredf[~ad_static_featuredf['adbusiid'].str.contains(',')]
	#转换时间格式
	ad_static_featuredf['createtime'] = ad_static_featuredf['createtime'].apply(lambda x:time.strftime('%Y%m%d%H%M%S',time.localtime(x)))

	ad_static_featuredf.to_csv(ad_static_feature_dfile, index=False)

#广告操作数据，生成csv文件，并清洗数据
def pre_ad_operation():
	ad_operationdf = pd.read_table(ad_operation_file,header=None, 
			names=['adid','optime','optype','modifyfield','fieldvalue'], low_memory=True)

	#删除广告id不在广告静态数据中的数据
	ad_static_featuredf = pd.read_csv(ad_static_feature_dfile, low_memory=True)
	adIdList = list(ad_static_featuredf['adid'])
	ad_operationdf = ad_operationdf[ad_operationdf['adid'].isin(adIdList)]

	ad_operationdf.to_csv(ad_operation_dfile, index=False)


#操作日志，生成csv文件，并清洗数据
def pre_total_exposure_Log():
	total_exposure_logdf = pd.read_table(total_exposure_log_file,header=None, 
			names=['adreqid','adreqtime','adlocalid','userid','adid','materialsize','bid','pctr','qualityecpm','totalecpm'], low_memory=True)
	
	#删除广告id不在广告静态数据中的数据
	ad_static_featuredf = pd.read_csv(ad_static_feature_dfile, low_memory=True)
	adIdList = list(ad_static_featuredf['adid'])
	total_exposure_logdf = total_exposure_logdf[total_exposure_logdf['adid'].isin(adIdList)]

	#转换时间格式
	total_exposure_logdf['adreqtime'] = total_exposure_logdf['adreqtime'].apply(lambda x:time.strftime('%Y%m%d%H%M%S',time.localtime(x)))

	#删除异常时间的数据如20190230这种
	#total_exposure_logdf = total_exposure_logdf[(total_exposure_logdf['adreqtime']<'20190228235959') & (total_exposure_logdf['adreqtime']>'20190301000000')]

	total_exposure_logdf.to_csv(total_exposure_log_dfile, index=False)

#将操作日志分解为多个小文件
def pre_total_exposure_Log_cut():
	csvfile = open(total_exposure_log_dfile, 'r')
	filename = 0
	i = 0
	for line in csvfile:
		if i % 10000000 == 0:
			fp = open(datapath +'totalExposureLog' + str(filename) + '.csv', 'w')
			fp.write(','.join(['adreqid','adreqtime','adlocalid','userid','adid','materialsize','bid','pctr','qualityecpm','totalecpm']) + '\n')
		if i == 0:
			line = ""
		fp.write(line)
		if i % 10000000 == 9999999:
			filename += 1
			fp.close()
		i += 1
	fp.close()
	csvfile.close()

#删除异常数据,去重,提取日期，并与后面按日期统计曝光率
def stat_exposure():
	#删除异常时间的数据如20190230这种
	for i in range(10):
		print i
		total_exposure_logdf = pd.read_csv(datapath + 'totalExposureLog' + str(i) + '.csv',  dtype = {'adreqtime' : str})
		total_exposure_logdf = total_exposure_logdf[(total_exposure_logdf['adreqtime']<'20190229000000') | (total_exposure_logdf['adreqtime']>'20190230235959')]
		total_exposure_logdf['date'] = total_exposure_logdf['adreqtime'].str[:8]
		total_exposure_logdf = total_exposure_logdf.drop_duplicates()
		total_exposure_logdf.to_csv(datapath + 'totalExposureLog' + str(i) + '.csv', index=False)


#统计每个广告id在每天内的曝光率，这个是label
def stat_exposure_group():
	for i in range(10):
		print i
		total_exposure_logdf = pd.read_csv(datapath + 'totalExposureLog' + str(i) + '.csv',  dtype = {'adreqtime' : str})
		gp = total_exposure_logdf.groupby(['adid','date'])
		newdf = gp.size()
		newdf.to_csv(datapath + 'totalExposureLogGroupgy' + str(i) + '.csv')






	#groupby
	#for i in range(1,10):
	#	print i
	#	total_exposure_logdf = pd.read_csv(datapath + 'totalExposureLog' + str(i) + '.csv', low_memory=True)
	#	total_exposure_logdf['date'] = total_exposure_logdf['adreqtime'][:8]
	#	total_exposure_logdf.to_csv(datapath + 'totalExposureLog' + str(i) + '.csv', index=False)



	

if __name__ == "__main__":
	#pre_ad_static_feature()
	#pre_ad_operation()
	#pre_total_exposure_Log()
	
	#pre_total_exposure_Log_cut()
	#stat_exposure()
	stat_exposure_group()



