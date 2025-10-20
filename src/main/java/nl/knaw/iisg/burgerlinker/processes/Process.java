package nl.knaw.iisg.burgerlinker.processes;


import java.util.Map;


public class Process {
    public enum ProcessType {
        BIRTH_DECEASED,
        BIRTH_MARRIAGE,
        DECEASED_MARRIAGE,
        MARRIAGE_MARRIAGE;
    }
    public enum RelationType{
        WITHIN,
        BETWEEN;
    }

    public ProcessType type;
    public RelationType rtype;
    public Map<String, String> dataModel;
    public String queryEventA, queryEventB;
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

        if (this.rtype == RelationType.WITHIN) {
            this.queryEventA = this.dataModel.get("BIRTHS");
            switch (this.type) {
                case BIRTH_DECEASED:
                    this.queryEventB = this.dataModel.get("DEATHS");

                    break;
                case BIRTH_MARRIAGE:
                    this.queryEventB = this.dataModel.get("MARRIAGES");

                    break;
            }
        } else {  // RelationType.BETWEEN
             switch (this.type) {
                case BIRTH_DECEASED:
                    this.queryEventA = this.dataModel.get("BIRTHS");
                    this.queryEventB = this.dataModel.get("DEATHS");

                    break;
                case BIRTH_MARRIAGE:
                    this.queryEventA = this.dataModel.get("BIRTHS");
                    this.queryEventB = this.dataModel.get("MARRIAGES");

                    break;
                case DECEASED_MARRIAGE:
                    this.queryEventA = this.dataModel.get("DEATHS");
                    this.queryEventB = this.dataModel.get("MARRIAGES");

                    break;
                case MARRIAGE_MARRIAGE:
                    this.queryEventA = this.dataModel.get("MARRIAGES");
                    this.queryEventB = this.dataModel.get("MARRIAGES");

                    break;
            }
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
            case BIRTH_MARRIAGE:
                processName += "B-M";

                break;
            case DECEASED_MARRIAGE:
                processName += "D-M";

                break;
            case MARRIAGE_MARRIAGE:
                processName += "M-M";

                break;
        }

        return processName;
    }

    public String toString() {
        String processName = "";
        switch (this.rtype) {
            case WITHIN:
                processName += "WITHIN";

                break;
            case BETWEEN:
                processName += "BETWEEN";

                break;
        }

        processName += "_";
        switch (this.type) {
            case BIRTH_DECEASED:
                processName += "B-D";

                break;
            case BIRTH_MARRIAGE:
                processName += "B-M";

                break;
            case DECEASED_MARRIAGE:
                processName += "D-M";

                break;
            case MARRIAGE_MARRIAGE:
                processName += "M-M";

                break;
        }

        return processName;
    }
}
