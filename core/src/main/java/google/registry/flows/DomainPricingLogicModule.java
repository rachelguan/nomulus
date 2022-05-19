package google.registry.flows;

import static com.google.common.base.Preconditions.checkState;

import dagger.Module;
import dagger.Provides;
import google.registry.flows.FlowModule.Superuser;
import google.registry.flows.custom.DomainPricingCustomLogic;
import google.registry.flows.domain.DomainPricingLogic;
import google.registry.model.eppinput.EppInput;
@Module
public class DomainPricingLogicModule {
  private EppInput eppInput;
  private SessionMetadata sessionMetadata;

  private DomainPricingLogicModule() {}
  @Provides
  EppInput provideEppInput() {
    return eppInput;
  }

  @Provides
  SessionMetadata provideSessionMetadata() {
    return sessionMetadata;
  }

  @Provides
  static FlowMetadata provideFlowMetadata(@Superuser boolean isSuperuser) {
    return FlowMetadata.newBuilder().setSuperuser(isSuperuser).build();
  }

  @Provides
  static DomainPricingCustomLogic provideDomainPricingCustomLogic(
      EppInput eppInput,
      SessionMetadata sessionMetadata,
      FlowMetadata flowMetadata) {
    return new DomainPricingCustomLogic(eppInput, sessionMetadata, flowMetadata);
  }

  @Provides
  static DomainPricingLogic provideDomainPricingLogic(
      EppInput eppInput,
      SessionMetadata sessionMetadata,
      FlowMetadata flowMetadata) {
    return new DomainPricingLogic(new DomainPricingCustomLogic(eppInput, sessionMetadata, flowMetadata));
  }


}
