package nl.knaw.iisg.burgerlinker.structs;

import nl.knaw.iisg.burgerlinker.structs.Person;


public class Couple {
    public final Person wife, husband;
    public final String familyCode;

    public Couple(Person wife, Person husband, String familyCode) {
        this.wife = wife;
        this.husband = husband;
        this.familyCode = familyCode;
    }
}
