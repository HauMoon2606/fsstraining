package Deserializer;

import DTO.Company;
import DTO.DbzCompany;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class JSONValueDeserializationDbzCompany implements DeserializationSchema<DbzCompany> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public DbzCompany deserialize(byte[] bytes) throws IOException {
        JsonNode rootNode = objectMapper.readTree(bytes);

        DbzCompany dbzCompany = new DbzCompany();
        dbzCompany.setBeforeData(objectMapper.treeToValue(rootNode.get("before"), Company.class));
        dbzCompany.setAfterData(objectMapper.treeToValue(rootNode.get("after"), Company.class));
        dbzCompany.setOp(rootNode.get("op").asText());
        return dbzCompany;
    }

    @Override
    public boolean isEndOfStream(DbzCompany dbzCompany) {
        return false;
    }

    @Override
    public TypeInformation<DbzCompany> getProducedType() {
        return TypeInformation.of(DbzCompany.class);
    }
}
