package nl.knaw.iisg.burgerlinker.structs;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import org.eclipse.rdf4j.model.Value;


public class Person {
	private String URI;
	private String role;
	private String first_name = null;
	private String last_name = null;
	private String gender = null;
	private Boolean valid;
	public final String names_separator = " ", compound_name_separator = "_";

    public Person(String event, String firstName, String familyName, String gender) {
		this.URI = event;
        this.first_name = firstName;
        this.last_name = familyName;
        this.gender = gender;

		this.valid = true;
    }

    public Person(String event, Value firstName, Value familyName, Value gender) {
		this.URI = event;
        this.first_name = (firstName != null) ? firstName.stringValue() : "";
        this.last_name = (familyName != null) ? familyName.stringValue() : "";
        this.gender = (gender != null) ? gender.stringValue() : "";

		this.valid = (URI != null);
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

	public String getFirstName() {
		if (first_name.length() > 0){
			String modified_first_name = first_name.replace(" ", compound_name_separator);
			modified_first_name = modified_first_name.replace("-", compound_name_separator);

			return modified_first_name;
		} else {
			return null;
		}
	}

	public String getLastName() {
		if (last_name.length() > 0) {
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
		if (firstName.contains(compound_name_separator)) {
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

	public String toString() {
        return URI + " (" + gender + "): " + last_name + "." + first_name;
	}

	public boolean hasFullName() {
		return (first_name.length() > 0
                && last_name.length() > 0
                && !first_name.equals("n"));
	}

	public boolean isFemale() {
		return gender.endsWith("Female");
    }

	public boolean isMale() {
		return gender.endsWith("Male");
    }

	public boolean hasGender(String gender) {
		return (this.gender.endsWith(gender) || this.gender.length() <= 0);
	}

	public boolean hasDoubleBarreledFirstName() {
		return (first_name.length() > 0
                && this.getFirstName().contains(compound_name_separator));
	}

	public String[] decomposeFirstname() {
		if (this.hasDoubleBarreledFirstName()) {
			return this.getFirstName().split(compound_name_separator);
		} else {
			return new String[] {this.getFirstName()};
		}
	}

	public boolean isValid() {
		return valid;
	}

	public boolean isValidWithFullName() {
		return (isValid() && hasFullName());
	}

	public void setValid(Boolean valid) {
		this.valid = valid;
	}

	public int getNumberOfFirstNames() {
		if (first_name.length() > 0) {
			int nb_first_names = StringUtils.countMatches(this.first_name, names_separator);

			return nb_first_names + 1;
		} else {
			return -1;
		}
	}
}
