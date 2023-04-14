package com.amazon.ata.advertising.service.businesslogic;

import com.amazon.ata.advertising.service.dao.CustomerProfileDao;
import com.amazon.ata.advertising.service.dao.ReadableDao;
import com.amazon.ata.advertising.service.model.*;
import com.amazon.ata.advertising.service.targeting.TargetingEvaluator;
import com.amazon.ata.advertising.service.targeting.TargetingGroup;

import com.amazon.ata.advertising.service.targeting.predicate.TargetingPredicateResult;
import com.amazon.ata.customerservice.CustomerProfile;
import com.amazon.ata.customerservice.GetCustomerProfileRequest;
import com.amazon.ata.customerservice.GetCustomerProfileResponse;
import com.amazon.atacustomerservicelambda.service.ATACustomerService;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;

/**
 * This class is responsible for picking the advertisement to be rendered.
 */
public class AdvertisementSelectionLogic {

    private static final Logger LOG = LogManager.getLogger(AdvertisementSelectionLogic.class);

    private final ReadableDao<String, List<AdvertisementContent>> contentDao;
    private final ReadableDao<String, List<TargetingGroup>> targetingGroupDao;
    private Random random = new Random();

    /**
     * Constructor for AdvertisementSelectionLogic.
     * @param contentDao Source of advertising content.
     * @param targetingGroupDao Source of targeting groups for each advertising content.
     */
    @Inject
    public AdvertisementSelectionLogic(ReadableDao<String, List<AdvertisementContent>> contentDao,
                                       ReadableDao<String, List<TargetingGroup>> targetingGroupDao) {
        this.contentDao = contentDao;
        this.targetingGroupDao = targetingGroupDao;

    }

    /**
     * Setter for Random class.
     * @param random generates random number used to select advertisements.
     */
    public void setRandom(Random random) {
        this.random = random;
    }

    /**
     * Gets all of the content and metadata for the marketplace and determines which content can be shown.  Returns the
     * eligible content with the highest click through rate.  If no advertisement is available or eligible, returns an
     * EmptyGeneratedAdvertisement.
     *
     * @param customerId - the customer to generate a custom advertisement for
     * @param marketplaceId - the id of the marketplace the advertisement will be rendered on
     * @return an advertisement customized for the customer id provided, or an empty advertisement if one could
     *     not be generated.
     */
    public GeneratedAdvertisement selectAdvertisement(String customerId, String marketplaceId) {

        TargetingEvaluator targetingEvaluator = new TargetingEvaluator(new RequestContext(customerId, marketplaceId));

        GeneratedAdvertisement generatedAdvertisement = new EmptyGeneratedAdvertisement();

        if (StringUtils.isEmpty(marketplaceId)) {
            LOG.warn("MarketplaceId cannot be null or empty. Returning empty ad.");
        }  else {

            final List<AdvertisementContent> contents = contentDao.get(marketplaceId);

            Optional<AdvertisementContent> ligibleAdContent  = contents.stream()
                    .flatMap(advertisementContent -> Stream.of(targetingGroupDao.get(advertisementContent.getContentId()))
                            .flatMap(targetingGroups -> targetingGroups.stream().sorted(Comparator.comparingDouble(TargetingGroup::getClickThroughRate))
                                    .filter(targetingGroup -> targetingEvaluator.evaluate(targetingGroup).isTrue())
                                    .map(targetingGroup -> advertisementContent))
                    ).findFirst();

           if(ligibleAdContent.isPresent()) {
              generatedAdvertisement = new GeneratedAdvertisement(ligibleAdContent.get());
           } else {
               return generatedAdvertisement;
           }

        }
        return generatedAdvertisement;
    }
}
