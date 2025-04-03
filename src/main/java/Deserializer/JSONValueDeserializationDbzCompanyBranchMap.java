package Deserializer;

import DTO.Company;
import DTO.CompanyBranchMap;
import DTO.DbzCompany;
import DTO.DbzCompanyBranchMap;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class JSONValueDeserializationDbzCompanyBranchMap implements DeserializationSchema<DbzCompanyBranchMap> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public DbzCompanyBranchMap deserialize(byte[] bytes) throws IOException {
        JsonNode rootNode = objectMapper.readTree(bytes);

        DbzCompanyBranchMap dbzCompanyBranchMap = new DbzCompanyBranchMap();
        dbzCompanyBranchMap.setBeforeData(objectMapper.treeToValue(rootNode.get("before"), CompanyBranchMap.class));
        dbzCompanyBranchMap.setAfterData(objectMapper.treeToValue(rootNode.get("after"), CompanyBranchMap.class));
        dbzCompanyBranchMap.setOp(rootNode.get("op").asText());
        return dbzCompanyBranchMap;
    }

    @Override
    public boolean isEndOfStream(DbzCompanyBranchMap dbzCompanyBranchMap) {
        return false;
    }

    @Override
    public TypeInformation<DbzCompanyBranchMap> getProducedType() {
        return TypeInformation.of(DbzCompanyBranchMap.class);
    }
}
