package com.athena.imis.models;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/***
 * 
 * A characteristic set is the list of properties that are common for a set of subject nodes  
 *
 */
public class CharacteristicSet {
	
	List<Integer> asList ;
	Set<String> matches ;
	ArrayList<int[]> triples;
	private boolean isDense = false ;
		
	
	
	public ArrayList<int[]> getTriples() {
		return triples;
	}

	public void setTriples(ArrayList<int[]> triples) {
		this.triples = triples;
	}

	
	public boolean isDense() {
		return isDense;
	}

	public void setDense(boolean isDense) {
		this.isDense = isDense;
	}

	public List<Integer> getAsList() {
		return asList;
	}

	public void setAsList(List<Integer> asList) {
		this.asList = asList;
	}

	public Set<String> getMatches() {
		return matches;
	}

	public void setMatches(Set<String> matches) {
		this.matches = matches;
	}
	
	public CharacteristicSet(List<Integer> asList){
		this.asList = asList;
		Collections.sort(this.asList);
	}
	
	public CharacteristicSet(Integer[] asArray){
		
		List<Integer> intList = new ArrayList<Integer>();
		for (int index = 0; index < asArray.length; index++)
		{
		    intList.add(asArray[index]);
		}
		this.asList = intList; 
		Collections.sort(this.asList);
	}
	
	public boolean contains (CharacteristicSet parent) {
		if (this.getAsList().containsAll(parent.getAsList()))
				return true;
		return false;
		
	}
	
	
	@Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
            // if deriving: appendSuper(super.hashCode()).
            append(asList).           
            toHashCode();
    }
	
	@Override
    public boolean equals(Object obj) {
       if (!(obj instanceof CharacteristicSet))
            return false;
        if (obj == this)
            return true;

        CharacteristicSet rhs = (CharacteristicSet) obj;
        return new EqualsBuilder().
            // if deriving: appendSuper(super.equals(obj)).
            append(asList, rhs.asList).            
            isEquals();
    }
	
	@Override
	public String toString(){
		return asList.toString();
	}

}
