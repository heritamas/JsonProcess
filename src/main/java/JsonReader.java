import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

class Person {
    @JsonProperty("name")
    public String name;
    @JsonProperty("salary")
    public Integer salary;
    @JsonIgnore
    public List<String> foods;
    @JsonIgnore
    public List<Object> vacations;

}

class DataRecord {
    @JsonProperty("event_id")
    public Integer event_id;

    @JsonProperty("data")
    public Person data;
}

public class JsonReader {

    public static void main(String[] args) throws IOException {

        //create object mapper
        ObjectMapper om = new ObjectMapper();
        List<String> errors = new ArrayList<>();
        List<DataRecord> records = new ArrayList<>();

        try (
                InputStream resourceAsStream = JsonReader.class.getResourceAsStream("/data.json");
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(resourceAsStream));
        ) {
            // read json string into java entity/POJO
            String line;
            while ( (line = bufferedReader.readLine()) != null) {
                try {
                    DataRecord dr = om.readValue(line, DataRecord.class);
                    records.add(dr);
                } catch (JsonMappingException e) {
                    // can't do anything, but drop & log
                    errors.add(String.format("%s in record %s", e.getMessage(), line));
                }
            }
        }

        // create a map from sorted events -> event sourcing stream/table "dichotomy"
        records.sort(Comparator.comparingInt(o -> o.event_id));
        Map<String, Optional<Integer>> salaries = records
                .stream()
                .map(rec -> rec.data)
                .filter(p -> p.salary != null)
                .collect(Collectors.groupingBy(
                        p -> p.name,
                        // we sorted by event id, so later salaries will overwrite earlier ones
                        Collectors.mapping(prs -> prs.salary, Collectors.reducing( (s1, s2) -> s2))));

        OptionalDouble valid = salaries
                .values()
                .stream()
                .flatMap(Optional::stream)
                .mapToDouble(Integer::doubleValue)
                .average();


        if (valid.isPresent())
            System.out.println("Average salary: " + valid.getAsDouble());

        errors.forEach(System.out::println);
    }
}
