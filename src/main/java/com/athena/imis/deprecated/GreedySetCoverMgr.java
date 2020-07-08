package com.athena.imis.deprecated;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.athena.imis.models.CharacteristicSet;

/**
 * Implements Vazirani's Greedy algo for greedy set cover. See Section 2.1 of approx. algo.book 
 * @author pvassil
 *
 */
public class GreedySetCoverMgr {
	private Set<CharacteristicSet> universe;					//U
	private List<Set<CharacteristicSet>> candidateSubsets;		//SS
	private List<Double> candidateSubsetCosts;					//cost of each S in SS

	private Set<CharacteristicSet> alreadyCovered;				//C, interim variable of elements already covered

	private ArrayList<Boolean> candidateSubsetIsPicked;			//belongs to solution or not
	/**
	 * @param universe
	 * @param candidateSubsets
	 * @param candidateSubsetCosts
	 */
	public GreedySetCoverMgr(Set<CharacteristicSet> universe, List<Set<CharacteristicSet>> candidateSubsets,
			List<Double> candidateSubsetCosts) {
		this.universe = universe;
		this.candidateSubsets = candidateSubsets;
		this.candidateSubsetCosts = candidateSubsetCosts;
		
		this.alreadyCovered = new HashSet<CharacteristicSet>();
		ArrayList<Boolean> candidateSubsetIsPicked = new ArrayList<Boolean>(candidateSubsets.size());
		for(int i = 0;  i< candidateSubsets.size(); i++ ) {
			candidateSubsetIsPicked.add(false);
		}
	}//end constructor

	
	
	public void findMinCostSubCollectionToCoverAllUniverse() {
		while(!checkAllCovered(universe, alreadyCovered)) {
			Set<CharacteristicSet> S0 = findCostEffective();
System.out.println("Picked:" + S0.toString());			
		}//endWhile
		
	}//end findMinCostSubCollectionToCoverAllUniverse()
	
	private Boolean checkAllCovered(Set<CharacteristicSet> setUniverse, Set<CharacteristicSet> setCovered) {
		if(setCovered.containsAll(setUniverse))
			return true;
		return false;
	}
	
	private Set<CharacteristicSet> findCostEffective(){
		Set<CharacteristicSet> S0 = null;
		Iterator<Set<CharacteristicSet>> iter = candidateSubsets.iterator();
		int position = 0;
		double aMin = Double.MAX_VALUE;
		int minPos = -1;
		while (iter.hasNext()) {
			Set<CharacteristicSet> currentCS = iter.next(); 
			int csSize = currentCS.size(); 
			int cSize =  alreadyCovered.size();
			double deltaCost = candidateSubsetCosts.get(position) / (Math.abs(cSize - csSize));
			if (deltaCost < aMin){
				minPos = position;
				aMin = deltaCost;
				S0 = currentCS;
			}
			position++;
		}
		this.candidateSubsetIsPicked.set(minPos, true);
		alreadyCovered.addAll(S0);
		return S0;
	}
	
	
}//end class
