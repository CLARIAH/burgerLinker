package nl.knaw.iisg.burgerlinker.structs;

import nl.knaw.iisg.burgerlinker.structs.Person;


public class Couple {
    public final Person mother, father;
    public final String familyCode;

    public Couple(Person mother, Person father, String familyCode) {
        /**
         * A simple class to model a married (?) couple
         **/
        this.mother = mother;
        this.father = father;
        this.familyCode = familyCode;
    }
}
