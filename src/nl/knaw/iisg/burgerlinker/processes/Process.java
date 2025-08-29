package nl.knaw.iisg.burgerlinker.processes;


import java.util.Map;


public class Process {
    public enum ProcessType {
        BIRTH_DECEASED,
        BIRTH_MARIAGE,
        DECEASED_MARIAGE,
        MARIAGE_MARIAGE;
    }
    public enum RelationType{
        WITHIN,
        BETWEEN;
    }

    public ProcessType type;
    public RelationType rtype;
    private Map<String, String> dataModel;
    public int minYearDiff, maxYeardiff;
    public String roleASubject,
                  roleASubjectFather,
                  roleASubjectMother,
                  roleBSubject,
                  roleBSubjectFather,
                  roleBSubjectMother,
                  roleBSubjectPartner,
                  roleBSubjectPartnerFather,
                  roleBSubjectPartnerMother;
    public final String csvHeader = "id_certificate_A,"
                                  + "id_certificate_B,"
                                  + "family_line,"
                                  + "levenshtein_total_AB,"
                                  + "levenshtein_max_AB,"
                                  + "levenshtein_total_AB_female_relative,"
                                  + "levenshtein_max_AB_female_relative,"
                                  + "levenshtein_total_AB_male_relative,"
                                  + "levenshtein_max_AB_male_relative,"
                                  + "matched_names_AB,"
                                  + "number_names_A,"
                                  + "number_names_B,"
                                  + "matched_names_AB_female_relative,"
                                  + "number_names_A_female_relative,"
                                  + "number_names_B_female_relative,"
                                  + "matched_names_subject_AB_male_relative,"
                                  + "number_names_subject_A_male_relative,"
                                  + "number_names_subject_B_male_relative,"
                                  + "year_diff";

    public Process(Map<String, String> dataModel) {
        this.dataModel = dataModel;
    }

    public Process(ProcessType type, RelationType rtype, Map<String, String> dataModel) {
        this.dataModel = dataModel;
        this.type = type;
        this.rtype = rtype;

        setValues(this.type, this.rtype);
    }

    public void setProcessType(ProcessType type) {
        this.type = type;
    }

     public void setRelationType(RelationType rtype) {
        this.rtype = rtype;
    }

   public void setValues(ProcessType type, RelationType rtype) {
        setProcessType(type);
        setRelationType(rtype);

        switch(type) {
            case BIRTH_DECEASED:
                this.roleASubject = dataModel.get("role_newborn");
                this.roleASubjectFather = dataModel.get("role_father");
                this.roleASubjectMother = dataModel.get("role_mother");
                this.roleBSubject = dataModel.get("role_deceased");
                this.roleBSubjectFather = dataModel.get("role_father");
                this.roleBSubjectMother = dataModel.get("role_mother");
                this.roleBSubjectPartner = dataModel.get("role_partner");
                this.roleBSubjectPartnerFather = null;
                this.roleBSubjectPartnerMother = null;

                if (rtype == Process.RelationType.WITHIN) {
                    this.minYearDiff = 0;
                    this.maxYeardiff = 110;
                } else {
                    this.minYearDiff = -1;
                    this.maxYeardiff = 96;
                }

                break;
            case BIRTH_MARIAGE:
                this.roleASubject = dataModel.get("role_newborn");
                this.roleASubjectFather = dataModel.get("role_father");
                this.roleASubjectMother = dataModel.get("role_mother");
                this.roleBSubject = dataModel.get("role_bride");
                this.roleBSubjectFather = dataModel.get("role_bride_father");
                this.roleBSubjectMother = dataModel.get("role_bride_mother");
                this.roleBSubjectPartner = dataModel.get("role_groom");
                this.roleBSubjectPartnerFather = dataModel.get("role_groom_father");
                this.roleBSubjectPartnerMother = dataModel.get("role_groom_mother");

                if (rtype == Process.RelationType.WITHIN) {
                    this.minYearDiff = 14;
                    this.maxYeardiff = 80;
                } else {
                    this.minYearDiff = -5;
                    this.maxYeardiff = 36;
                }

                break;
            case DECEASED_MARIAGE:
                this.roleASubject = dataModel.get("role_deceased");
                this.roleASubjectFather = dataModel.get("role_father");
                this.roleASubjectMother = dataModel.get("role_mother");
                this.roleBSubject = dataModel.get("role_bride");
                this.roleBSubjectFather = null;
                this.roleBSubjectMother = null;
                this.roleBSubjectPartner = dataModel.get("role_groom");
                this.roleBSubjectPartnerFather = null;
                this.roleBSubjectPartnerMother = null;

                this.minYearDiff = -5;
                this.maxYeardiff = 146;

                break;
            case MARIAGE_MARIAGE:
                this.roleASubject = dataModel.get("role_bride");
                this.roleASubjectFather = dataModel.get("role_bride_father");
                this.roleASubjectMother = dataModel.get("role_bride_mother");
                this.roleBSubject = dataModel.get("role_bride");
                this.roleBSubjectFather = null;
                this.roleBSubjectMother = null;
                this.roleBSubjectPartner = dataModel.get("role_groom");
                this.roleBSubjectPartnerFather = dataModel.get("role_groom_father");
                this.roleBSubjectPartnerMother = dataModel.get("role_groom_mother");

                this.minYearDiff = 14;
                this.maxYeardiff = 100;

                break;
        }
    }

    public String abbr() {
        String processName = "";
        switch (this.rtype) {
            case WITHIN:
                processName += "W";

                break;
            case BETWEEN:
                processName += "B";

                break;
        }

        processName += "_";
        switch (this.type) {
            case BIRTH_DECEASED:
                processName += "B-D";

                break;
            case BIRTH_MARIAGE:
                processName += "B-M";

                break;
            case DECEASED_MARIAGE:
                processName += "D-M";

                break;
            case MARIAGE_MARIAGE:
                processName += "M-M";

                break;
        }

        return processName;
    }

    public String toString() {
        String processName = "";
        switch (this.rtype) {
            case WITHIN:
                processName += "Within";

                break;
            case BETWEEN:
                processName += "Between";

                break;
        }

        processName += "_";
        switch (this.type) {
            case BIRTH_DECEASED:
                processName += "B-D";

                break;
            case BIRTH_MARIAGE:
                processName += "B-M";

                break;
            case DECEASED_MARIAGE:
                processName += "D-M";

                break;
            case MARIAGE_MARIAGE:
                processName += "M-M";

                break;
        }

        return processName;
    }
}
