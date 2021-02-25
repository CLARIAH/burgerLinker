package iisg.amsterdam.wp4_links;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

public class Person {
	private String URI;
	private String role;
	private String first_name = null;
	private String last_name = null;
	private String gender = null;
	private Boolean valid;
	public final String names_separator = " ", compound_name_separator = "_";

	public Person() {
		setValid(false);
	}

	public Person(CharSequence URI, String role) {
		this.URI = URI.toString();
		this.role = role;
		setValid(true);
	}

	public void setFirstName(String first_name){
		this.first_name = first_name;
	}

	public void setLastName(String last_name){
		this.last_name = last_name;
	}

	public void setGender(String gender){
		this.gender = gender;
	}

	public String getURI() {
		return URI;
	}

	public String getRole() {
		return role;
	}

	public String getFirstName() {
		if(first_name!=null){
			String modified_first_name = first_name.replace(" ", compound_name_separator);
			modified_first_name = modified_first_name.replace("-", compound_name_separator);
			return modified_first_name;
		} else {
			return null;
		}		
	}

	public String getLastName() {
		if(last_name!=null) {
			String modified_last_name = last_name.replace(" ", compound_name_separator);
			modified_last_name = modified_last_name.replace("-", compound_name_separator);
			return modified_last_name;
		} else {
			return null;
		}	
	}

	public String getFullName() {
		String fullName = getFirstName() + names_separator + getLastName();
		return fullName;
	}


	public HashMap<String, String> getPossibleFullNameCombinations(){
		HashMap<String,String> fullNames = new HashMap<String,String>();
		String firstName = getFirstName();
		if(firstName.contains(compound_name_separator)) {
			String[] firstNames = firstName.split(compound_name_separator);
			fullNames = orderedCombination(firstNames, getLastName());
		} else {
			fullNames.put(getFullName(), "1/1");
		}
		return fullNames;
	}


	public HashMap<String,String> orderedCombination(String[] firstNames, String lastName) {
		HashMap<String,String> result = new HashMap<String,String>();
		Integer length = firstNames.length;
		String[] copiedFirstNames = firstNames.clone();
		for (int i=0; i< length; i++){
			String fixed = firstNames[i];
			int count = 1;
			result.put(fixed + names_separator + lastName, count+ "/" +length);
			copiedFirstNames = ArrayUtils.removeElement(copiedFirstNames, fixed);
			for (String fn: copiedFirstNames){
				count++;
				fixed = fixed + compound_name_separator + fn;
				result.put(fixed + names_separator + lastName, count + "/" + length);
			}
		}
		return result;
	}



	public String getGender() {
		return gender;
	}

	public void printIdentity() {
		System.out.println("+-------------------------------------------------------------------------------");
		System.out.println("* Person: " + URI);
		System.out.println(" ---> First Name: " + first_name);
		System.out.println(" ---> Last Name: " + last_name);
		System.out.println(" ---> Role: " + role);
		System.out.println(" ---> Gender: " + gender);
		System.out.println("+-------------------------------------------------------------------------------");
	}

	public Boolean hasFirstName() {
		if(first_name != null){
			return true;
		} else {
			return false;
		}
	}

	public Boolean hasLastName() {
		if(last_name != null){
			return true;
		} else {
			return false;
		}
	}

	public Boolean hasFullName() {
		if(first_name != null && !first_name.equals("n")){
			if(last_name != null) {
				return true;
			}
		}
		return false;
	}

	public Boolean isFemale() {
		if(gender.equals("f")){
			return true;
		}
		return false;
	}

	public Boolean hasDoubleBarreledFirstName() {
		if (this.getFirstName() != null) {
			if(this.getFirstName().contains(compound_name_separator)) {
				return true;
			} 
		}
		return false;
	}


	public Boolean hasDoubleBarreledLastName() {
		if (this.getLastName() != null) {
			if(this.getLastName().contains(compound_name_separator)) {
				return true;
			} 
		}
		return false;
	}

	public String[] decomposeFirstname() {
		if(this.hasDoubleBarreledFirstName()) {
			return this.getFirstName().split(compound_name_separator);
		} else {
			return new String[] {this.getFirstName()};
		}
	}


	public Set<String> decomposeFirstnameAddLastName() {
		Set<String> result = new HashSet<String>();
		if(this.hasDoubleBarreledFirstName()) {
			String[] firstNames = this.getFirstName().split(compound_name_separator); 
			String lastname = this.getLastName(); 
			for(String firstname: firstNames) {
				result.add(firstname + names_separator + lastname);
			}
		} else {
			result.add(this.getFullName());
		}
		return result;
	}



	public String[] decomposeLastname() {
		return this.getLastName().split(compound_name_separator);
	}

	public Boolean isValid() {
		return valid;
	}

	public Boolean isValidWithFullName() {
		if(isValid()) {
			if(hasFullName()) {
				return true;
			}
		}
		return false;
	}

	public void setValid(Boolean valid) {
		this.valid = valid;
	}

	
	public int getNumberOfFirstNames() {
		if(hasFirstName()) {
			int nb_first_names = StringUtils.countMatches(this.first_name, names_separator);
			return nb_first_names + 1;
		} else {
			return -1;
		}
	}

}
