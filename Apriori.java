package hadoop_project;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Apriori {
	
	ArrayList<HashSet<Integer>> candidateItemsets = new ArrayList<HashSet<Integer>>();
	ArrayList<ArrayList<HashSet<Integer>>> allFrequentItems = new ArrayList<ArrayList<HashSet<Integer>>>();
	Hashtable<HashSet<Integer>, Integer> itemsetCount = new Hashtable<HashSet<Integer>, Integer>();
	String[] allBaskets;
	Integer numberBaskets;
	Integer threshold;
	Integer itemCount=1;
	
	private void processBasketInFirstPass(String line){
		
		String[] items = line.split(" ");
		HashSet<Integer> tempList = new HashSet<Integer>();
		
		for (String item: items){
			
			tempList = new HashSet<Integer>();
			if (!item.equals("")){
			tempList.add(Integer.parseInt(item));
			if (itemsetCount.containsKey(tempList)){
				itemsetCount.put(tempList, itemsetCount.get(tempList)+1);
			}
			else{
				itemsetCount.put(tempList, 1);
			}
			}
			
		}
		
	}
	
	private boolean filterOutFrequentItemsets(){
		
		Set<HashSet<Integer>> keys = itemsetCount.keySet();
		
		ArrayList<HashSet<Integer>> tempList = new ArrayList<HashSet<Integer>>();
		
		for (HashSet<Integer> key: keys){
			
			if (itemsetCount.get(key)>=threshold){
				tempList.add(key);
			}
			
		}
		if (tempList.size()>0){
		allFrequentItems.add(tempList);
		return true;
		}
		return false;
		
	}
	
 	private HashSet<Integer> getAllOccuringItemsFromList(ArrayList<HashSet<Integer>> list){
 		
 		HashSet<Integer> allOccuringItems = new HashSet<Integer>();
 		
 		for(HashSet<Integer> itemset: list){
 			
 			for(Integer item:itemset){
 				
 				if (!allOccuringItems.contains(item)){
 					allOccuringItems.add(item);
 				}
 				
 			}
 			
 		}
 		
		return allOccuringItems;
 	}
 	
 	private ArrayList<HashSet<Integer>> getAllNTupelsFromList(HashSet<Integer> list, Integer n){
 		
 		ArrayList<HashSet<Integer>> allNTupels = new ArrayList<HashSet<Integer>>();
 		
 		if (list.size()<n){
 			return allNTupels;
 		}
 		
 		HashSet<Integer> tempList = new HashSet<Integer>();
 		Integer numberOfItems=list.size();
 		Integer[] selectedItems= new Integer[n];
 		
 		for (int i=0;i<n;i++){
 			selectedItems[i]=i+1;
 		}
 		boolean allItemsetsAdded=false;
 		Integer pointerToUpdate=n-1;
 		Integer counter=0;
 		tempList = new HashSet<Integer>();
 		Integer[] listAsArray = list.toArray(new Integer[list.size()]);
 		
		for (int i=0;i<n;i++){
			tempList.add(listAsArray[selectedItems[i]-1]);
		}
		allNTupels.add(tempList);
		
 		while(!allItemsetsAdded){
 			
 			while(selectedItems[pointerToUpdate]==numberOfItems-(n-(pointerToUpdate+1))){
 				pointerToUpdate--;
 				if (pointerToUpdate==-1)
 					break;
 			}
 			if (pointerToUpdate>=0){
 				selectedItems[pointerToUpdate]++;
 				counter=1;
 				for (int i=pointerToUpdate+1;i<n;i++){
 					selectedItems[i]=selectedItems[pointerToUpdate]+counter;
 					counter++;
 				}
 				pointerToUpdate=n-1;
 				tempList = new HashSet<Integer>();
 				for (int i=0;i<n;i++){
 					tempList.add(listAsArray[selectedItems[i]-1]);
 				}
 				allNTupels.add(tempList);
 			}
 			else{
 				allItemsetsAdded=true;
 			}
 			
 		}
		return allNTupels;
 	}
 	
 	private ArrayList<HashSet<Integer>> getAllCandidatesFromPreviousListOfFrequentItems(ArrayList<HashSet<Integer>> previousList){
		
 		Integer sizeOfPreviousList = previousList.size();
 		Integer itemsInPreviousList = previousList.get(0).size();
 		Integer numberItemsInNewList = itemsInPreviousList+1;
 		Integer[] selectedLists = new Integer[numberItemsInNewList];
 		Integer pointerToUpdate=numberItemsInNewList-1;
 		for (int i=0;i<numberItemsInNewList;i++){
 			selectedLists[i]=i+1;
 		}

 		Integer counter;
 		ArrayList<HashSet<Integer>> tempLists = new ArrayList<HashSet<Integer>>();
 		HashSet<Integer> tempSet;
 		ArrayList<HashSet<Integer>> allCandidates = new ArrayList<HashSet<Integer>>();
 		boolean allListsChecked=false;

 		if (previousList.size()<numberItemsInNewList)
 			return allCandidates;
			
 		while (!allListsChecked){
 			
 			tempLists.clear();
 			for (int i=0;i<numberItemsInNewList;i++){
 				tempLists.add(previousList.get(selectedLists[i]-1));
 			}
 			tempSet = new HashSet<Integer>();
 			tempSet = getAllOccuringItemsFromList(tempLists);
 			if (tempSet.size()==numberItemsInNewList){
 				allCandidates.add(tempSet);

 			}
 			while(selectedLists[pointerToUpdate]==sizeOfPreviousList-(numberItemsInNewList-(pointerToUpdate+1))){
 				pointerToUpdate--;
 				if (pointerToUpdate==-1)
 					break;
 			}
 			if (pointerToUpdate>=0){
 				selectedLists[pointerToUpdate]++;
 				counter=1;
 				for (int i=pointerToUpdate+1;i<numberItemsInNewList;i++){
 					selectedLists[i]=selectedLists[pointerToUpdate]+counter;
 					counter++;
 				}
 				pointerToUpdate=numberItemsInNewList-1;
 			}
 			else{
 				allListsChecked=true;
 			}
 		}
 		
 		
 		return allCandidates;
 	}

	private boolean firstPass() throws IOException{
		
		boolean itemsetAdded=false;
		Integer counter=0;
		while (counter<numberBaskets){
			
			processBasketInFirstPass(allBaskets[counter]);
			counter++;
		}
		itemsetAdded=filterOutFrequentItemsets();
		
		
		return itemsetAdded;
	}
	
	private void processBasketForCandidateItemsets(String line) {
		
		HashSet<Integer> itemsAsInt = new HashSet<Integer>();
		String[] items = line.split(" ");
		for (String item: items){
			itemsAsInt.add(Integer.parseInt(item));
		}
		
		for (HashSet<Integer> candidate: candidateItemsets){
			boolean allItemsOfSetInBasket=true;
			for (Integer item: candidate){
				
				if(!itemsAsInt.contains(item)){
					allItemsOfSetInBasket=false;
					break;
				}
				
			}
			if (allItemsOfSetInBasket){
				if (itemsetCount.containsKey(candidate)){
					itemsetCount.put(candidate, itemsetCount.get(candidate)+1);
				}
				else{
					itemsetCount.put(candidate, 1);
				}
			}
			
		}
		
	}

	public boolean nextPass() throws IOException{
		
		boolean itemsetAdded=false;
		
		String line;
		itemsetCount.clear();
		int counter=1;
		int counter2=0;
		//System.out.print("checking basket: ");
		while (counter2<numberBaskets){
			processBasketForCandidateItemsets(allBaskets[counter2]);
			counter++;
			
				//System.out.print(counter+" ");
			
				
			counter2++;
		}
		//System.out.println("");
		//System.out.println("filtering out frequent "+itemCount+"-itemsets..");
		itemsetAdded=filterOutFrequentItemsets();
		
		

		return itemsetAdded;
		
	}
	
	private void readFile(String fileName) throws IOException{
		
		String path = fileName;
		FileReader fr = new FileReader(path);
		BufferedReader textReader = new BufferedReader(fr);
		String line;
		Integer counter=0;
		while ((line = textReader.readLine())!=null){
			counter++;
		}
		numberBaskets=counter;
		textReader.close();

		allBaskets = new String[counter];
		fr = new FileReader(path);
		textReader = new BufferedReader(fr);
		counter=0;
		while ((line = textReader.readLine())!=null){
			allBaskets[counter]=line;
			counter++;
		}
	}
	
	public void aprioriCalculation(String fileName, Integer threshold) throws IOException{
		
		readFile(fileName);
		this.threshold=threshold;
		boolean itemsetAdded;
		itemsetAdded=firstPass();
		if (itemsetAdded){
		//System.out.println("frequent "+itemCount+"-itemsets ("+allFrequentItems.get(itemCount-1).size()+") : "+allFrequentItems.get(itemCount-1));
		candidateItemsets=getAllCandidatesFromPreviousListOfFrequentItems(allFrequentItems.get(0));
		//System.out.println(candidateItemsets.size()+" candidates for next pass..");
		}
		
		
		while (itemsetAdded && candidateItemsets.size()>0){
			itemCount++;
			itemsetAdded=nextPass();
			
			if (itemsetAdded){
				//System.out.println("frequent "+itemCount+"-itemsets ("+allFrequentItems.get(itemCount-1).size()+") : "+allFrequentItems.get(itemCount-1));
				if (allFrequentItems.get(itemCount-1).size()>0){
					candidateItemsets=getAllCandidatesFromPreviousListOfFrequentItems(allFrequentItems.get(itemCount-1));
					//System.out.println(candidateItemsets.size()+" candidates for next pass..");
				}
			}
			
		}
			
		
		
		
	}
	
	private void readBaskets(String[] baskets) throws IOException{
		
		
		numberBaskets=baskets.length;

		allBaskets = new String[numberBaskets];
		for (int i=0;i<numberBaskets;i++){
			allBaskets[i]=baskets[i];
		}
			
	}
	
	public ArrayList<ArrayList<HashSet<Integer>>> aprioriCalculation(String[] baskets, Integer threshold) throws IOException{
		
		readBaskets(baskets);
		this.threshold=threshold;
		boolean itemsetAdded;
		itemsetAdded=firstPass();
		if (itemsetAdded){
		//System.out.println("frequent "+itemCount+"-itemsets ("+allFrequentItems.get(itemCount-1).size()+") : "+allFrequentItems.get(itemCount-1));
		candidateItemsets=getAllCandidatesFromPreviousListOfFrequentItems(allFrequentItems.get(0));
		//System.out.println(candidateItemsets.size()+" candidates for next pass..");
		}
		
		
		while (itemsetAdded && candidateItemsets.size()>0){
			itemCount++;
			itemsetAdded=nextPass();
			
			if (itemsetAdded){
				//System.out.println("frequent "+itemCount+"-itemsets ("+allFrequentItems.get(itemCount-1).size()+") : "+allFrequentItems.get(itemCount-1));
				if (allFrequentItems.get(itemCount-1).size()>0){
					candidateItemsets=getAllCandidatesFromPreviousListOfFrequentItems(allFrequentItems.get(itemCount-1));
					//System.out.println(candidateItemsets.size()+" candidates for next pass..");
				}
			}
			
		}
			
		return allFrequentItems;
		
		
	}
	
	public void test(){
		
	
		
	}
	
	
	

}