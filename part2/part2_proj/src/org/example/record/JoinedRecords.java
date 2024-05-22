package org.example.record;

import org.example.metadata.AttributeMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class JoinedRecords {
    List<AttributeMetadata> attrMetadataList;
    List<Record> resultSet;

    public List<AttributeMetadata> getAttrMetadataList() {
        return attrMetadataList;
    }

    public List<Record> getResultSet() {
        return resultSet;
    }

    public JoinedRecords(List<AttributeMetadata> attrMetadataListR,
                         List<AttributeMetadata> attrMetadataListS,
                         List<String> joinAttrs,
                         HashMap<String, Integer> joinAttrPosS,
                         List<MatchedRecord> matchedRecords) {


        List<AttributeMetadata> joinedAttrMetadataListR = List.copyOf(attrMetadataListR);
        List<AttributeMetadata> joinedAttrMetadataListS = new ArrayList<>();
        attrMetadataList = new ArrayList<>();

        for (AttributeMetadata attrMetadata : attrMetadataListS) { // copy attrMetadataListS except join attributes
            String attributeName = attrMetadata.getAttributeName();
            if(joinAttrs.contains(attributeName)) {
                continue;
            }

            joinedAttrMetadataListS.add(new AttributeMetadata(attrMetadata.getRelationName(),
                    attrMetadata.getAttributeName(),
                    attrMetadata.getDomainType(),
                    attrMetadata.getPosition(),
                    attrMetadata.getLength(),
                    attrMetadata.isPrimary(),
                    null,
                    null));
        }


        for(int i = 0; i < joinedAttrMetadataListR.size(); i++) {
            for(int j = 0; j < joinedAttrMetadataListS.size(); j++) {
                AttributeMetadata attributeMetadataR = joinedAttrMetadataListR.get(i);
                AttributeMetadata attributeMetadataS = joinedAttrMetadataListS.get(j);

                String attributeNameR = attributeMetadataR.getAttributeName();
                String attributeNameS = attributeMetadataS.getAttributeName();

                if(attributeNameR.equals(attributeNameS)) { // when two attribute name has same name, distinguish them by annotating relation name
                    attributeMetadataR.setAttributeName(attributeMetadataR.getRelationName() + "." + attributeNameR);
                    attributeMetadataS.setAttributeName(attributeMetadataS.getRelationName() + "." + attributeNameS);
                }
            }
        }

        // Concatenate attributeMetadata
        for (AttributeMetadata attributeMetadata : joinedAttrMetadataListR) {
            attrMetadataList.add(attributeMetadata);
        }
        for (AttributeMetadata attributeMetadata : joinedAttrMetadataListS) {
            attrMetadataList.add(attributeMetadata);
        }

        resultSet = new ArrayList<>();
        for (MatchedRecord matchedRecord : matchedRecords) {
            List<char[]> attributes = new ArrayList<>();
            List<char[]> attributesR = matchedRecord.getRecordR().getAttributes();
            attributes.addAll(attributesR);


            Collection<Integer> pos = joinAttrPosS.values();
            List<char[]> attributesS = matchedRecord.getRecordS().getAttributes();
            for(int i = 0; i < attributesS.size(); i++) {
                if(pos.contains(i)) { // Exclude join column
                    continue;
                }
                attributes.add(attributesS.get(i));
            }
            resultSet.add(new Record(attributes, null));
        }
    }
}
