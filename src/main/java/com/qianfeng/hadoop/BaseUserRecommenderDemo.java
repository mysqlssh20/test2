package com.qianfeng.hadoop;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.precompute.example.GroupLensDataModel;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * 第一代协同过滤
 *
 *
 * */
public class BaseUserRecommenderDemo {
    public static void main(String[] args) throws IOException, TasteException {
        //1.准备数据，这里是电影评分数据
        File file  = new File("G:\\BigData-GP-09\\ratings.dat");

        //2.将数据加载到内存
        DataModel dataMode = new GroupLensDataModel(file);

        //3.计算用户的相似度，相似度的算法有很多种，采用皮尔逊算法计算相似度
        PearsonCorrelationSimilarity similarity  = new PearsonCorrelationSimilarity(dataMode);

        //4.计算最近的邻域 ，主要有两种方法，一种是固定的数量，一种是固定的阈值，采用固定的数量
        NearestNUserNeighborhood userNeighborhood = new NearestNUserNeighborhood(100, similarity, dataMode);


        //5.构造推荐器
       Recommender recommender = new GenericUserBasedRecommender(dataMode, userNeighborhood, similarity);

        //6.给用户2265推荐10部电影
        List<RecommendedItem> itemList = recommender.recommend(2265, 10);


        //7.d打印推荐结果
        for (RecommendedItem item :itemList){
            System.out.println(item);
        }


    }
}
