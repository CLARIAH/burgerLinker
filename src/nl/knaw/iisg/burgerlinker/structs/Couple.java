package nl.knaw.iisg.burgerlinker.structs;

import nl.knaw.iisg.burgerlinker.structs.Person;


public class Couple {
    public final Person partnerA, partnerB;
    public final String familyCode;

    public Couple(Person partnerA, Person partnerB, String familyCode) {
        /**
         * A simple class to model a married (?) couple
         **/
        this.partnerA = partnerA;
        this.partnerB = partnerB;
        this.familyCode = familyCode;
    }
}
