package com.predictionmarketing.RecommenderApp;

import java.io.File;
import java.util.List;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/**
 * Hello world!
 *
 */
public class App 
{
	public static void main(String[] args) throws Exception {
		DataModel model = new FileDataModel(new File("data/dataset.csv"));
		
		
		
		ItemSimilarity itemSimilarity= new PearsonCorrelationSimilarity(model);
		ItemBasedRecommender  itemRecommender = new GenericItemBasedRecommender(model, itemSimilarity);
		List<RecommendedItem> itemRecommendations =itemRecommender.recommend(2,10);
		for (RecommendedItem itemRecommendation : itemRecommendations)
		System.out.println("ItemSimilarity :" + itemRecommendation);
		
	}
}









/*UserSimilarity similarity =    new PearsonCorrelationSimilarity(model);
UserNeighborhood neighborhood = new NearestNUserNeighborhood(5,similarity, model);
Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
List<RecommendedItem> recommendations =recommender.recommend(2, 3);
for (RecommendedItem recommendation : recommendations)
System.out.println("UserSimilarity :" + recommendation);*/